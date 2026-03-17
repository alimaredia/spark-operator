## This contains the core Python function (docling processor) that handles the actual document processing.
## Zero PySpark imports or spark-specific code.

# standard imports
import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
import json

# Docling imports
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
from docling_core.types.doc import ImageRefMode

@dataclass
class ProcessingResult:
    """
    Encapsulates the result of document processing.
    Attributes:
    - success (bool): Whether the processing was successful.
    - content (str): The processed content of the document (markdown format).
    - json_content (str): The JSON content of the document (docling JSON format).
    - metadata (Dict): Additional metadata about the document.
    - error_message (Optional[str]): Error message if processing failed.
    - file_path (str): Original file path that was processed.
    """
    success: bool
    content: str
    json_content: str
    metadata: Dict[str, Any]
    error_message: Optional[str]
    file_path: str

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the ProcessingResult to a dictionary.
        """
        return {
            "success": self.success,
            "content": self.content,
            "json_content": self.json_content,
            "metadata": self.metadata,
            "error_message": self.error_message,
            "file_path": self.file_path,
        }

    def __str__(self) -> str:
        """
        Return a string representation of the ProcessingResult.
        """
        status = "SUCCESS" if self.success else "FAILED"
        return f"ProcessingResult(success={status}, file_path={self.file_path})"

@dataclass
class DocumentConfig:
    """
    Configuration for document processing. OCR Engines available (when ocr_enabled=True):
    - "rapidocr": Lightweight pure-Python OCR (recommended)
    - "tesserocr": Fast C-binding for tesseract
    - "tesseract": CLI wrapper via pytesseract
    """
    # Basic configuration options
    extract_tables: bool = True
    extract_images: bool = True
    ocr_enabled: bool = False  # Disabled by default for lightweight processing
    max_pages: Optional[int] = None

    # Advanced configuration options
    pdf_backend: str = "dlparse_v4"
    image_export_mode: str = "embedded" 
    table_mode: str = "accurate"
    num_threads: int = 4  
    timeout_per_document: int = 300
    ocr_engine: str = "rapidocr"  # Options: "rapidocr", "tesserocr", "tesseract"
    force_ocr: bool = False

    # Enrichment options 
    enrich_code: bool = False  
    enrich_formula: bool = False
    enrich_picture_classes: bool = False
    enrich_picture_description: bool = False

    # Performance options (auto, cpu, gpu)
    accelerator_device: str = "cpu" 

class DocumentProcessorInterface(ABC):
    """
    Interface for document processors.
    """
    @abstractmethod
    def process(self, file_path: str) -> ProcessingResult:
        """
        Process a document and return the result.
        """
        pass
    
    @abstractmethod
    def validate_file(self, file_path: str) -> bool:
        """
        Validate a file before processing.
        """
        pass

class DoclingPDFProcessor(DocumentProcessorInterface):
    """
    Processor for PDF documents.
    """
    def __init__(self, config: Optional[DocumentConfig] = None):
        """
        Initialize the DoclingPDFProcessor.
        """
        self._config = config if config else DocumentConfig()
        # Initialize the docling converter
        self._converter = self._initialize_converter()
        # class level constants (immutable)
        self._supported_extensions = ['.pdf']

    def _initialize_converter(self) -> DocumentConverter:
        """
        Initialize the Docling converter.
        """
        # Create pipeline options based on the configuration
        pipeline_options = PdfPipelineOptions()
        
        # Basic options
        pipeline_options.do_table_structure = self._config.extract_tables
        pipeline_options.do_ocr = self._config.ocr_enabled
        pipeline_options.generate_page_images = self._config.extract_images
        
        # Advanced options from data-processing repo
        pipeline_options.table_structure_options.do_cell_matching = True
        pipeline_options.document_timeout = float(self._config.timeout_per_document)
        
        # Enrichment options
        pipeline_options.do_code_enrichment = self._config.enrich_code
        pipeline_options.do_formula_enrichment = self._config.enrich_formula
        pipeline_options.do_picture_classification = self._config.enrich_picture_classes
        pipeline_options.do_picture_description = self._config.enrich_picture_description
        
        # Accelerator options (simplified: auto, cpu, gpu)
        device_map = {
            "auto": AcceleratorDevice.AUTO,
            "cpu": AcceleratorDevice.CPU,
            "gpu": AcceleratorDevice.CUDA,
        }
        device = device_map.get(self._config.accelerator_device.lower(), AcceleratorDevice.AUTO)
        
        pipeline_options.accelerator_options = AcceleratorOptions(
            num_threads=self._config.num_threads,
            device=device
        )

        # OCR options - Multi-engine support
        if self._config.ocr_enabled:
            engine = self._config.ocr_engine.lower()
            
            if engine == "rapidocr":
                # Lightweight pure-Python OCR (recommended)
                from docling.datamodel.pipeline_options import RapidOcrOptions
                pipeline_options.ocr_options = RapidOcrOptions(
                    force_full_page_ocr=self._config.force_ocr
                )
            elif engine == "tesserocr":
                # Fast C-binding for tesseract
                from docling.datamodel.pipeline_options import TesseractOcrOptions
                pipeline_options.ocr_options = TesseractOcrOptions(
                    force_full_page_ocr=self._config.force_ocr
                )
            elif engine == "tesseract":
                # CLI wrapper via pytesseract
                from docling.datamodel.pipeline_options import TesseractCliOcrOptions
                pipeline_options.ocr_options = TesseractCliOcrOptions(
                    force_full_page_ocr=self._config.force_ocr
                )
            else:
                raise ValueError(f"Unknown OCR engine: {engine}. Supported: rapidocr, tesserocr, tesseract")

        # Create and return the converter 
        converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                    backend=DoclingParseV4DocumentBackend,  
                    pipeline_cls=StandardPdfPipeline,
                )
            }
        )
        return converter

    def validate_file(self, file_path: str) -> bool:
        """
        Validate a file before processing.
        """
        path_obj = Path(file_path)

        # File must exist
        if not path_obj.exists():
            return False

        # Must be a file (not a directory)
        if not path_obj.is_file():
            return False

        # Must have a supported extension
        if path_obj.suffix.lower() not in self._supported_extensions:
            return False
        
        return True

    def process(self, file_path: str) -> ProcessingResult:
        """
        Process a document and return the result.
        """
        try:
            # Validate the file
            if not self.validate_file(file_path):
                return ProcessingResult(
                    success=False,
                    content="",
                    json_content="",
                    metadata={},
                    error_message="Invalid file",
                    file_path=file_path
                )

            # Convert the document using docling
            result = self._converter.convert(file_path)

            # Extract content using modern export methods
            # 1. Markdown content
            markdown_content = result.document.export_to_markdown(
                image_mode=ImageRefMode(self._config.image_export_mode)
            )
            
            # 2. JSON content (docling's native JSON format for docling serve)
            json_content = result.document.export_to_dict()
            json_content_str = json.dumps(json_content, ensure_ascii=False)
            
            # Extract metadata
            metadata = self._extract_metadata(result, file_path)
            
            # Return success result
            return ProcessingResult(
                success=True,
                content=markdown_content,
                json_content=json_content_str,
                metadata=metadata,
                error_message=None,
                file_path=file_path
            )
        except Exception as e:
            # Error handling - return failure result with error message
            import traceback
            full_error = f"Error processing {file_path}: {str(e)}\nTraceback: {traceback.format_exc()}"
            return ProcessingResult(
                success=False,
                content="",
                json_content="",
                metadata={},
                error_message=full_error,
                file_path=file_path
            )

    def _extract_metadata(self, docling_result, file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from the Docling result.
        """
        path_obj = Path(file_path)
        
        # Extract confidence score safely
        confidence_score = 0.0
        if hasattr(docling_result, 'confidence'):
            confidence_obj = docling_result.confidence
            # Try different ways to access the confidence score
            if hasattr(confidence_obj, 'mean_score'):
                confidence_score = confidence_obj.mean_score
            elif isinstance(confidence_obj, dict):
                confidence_score = confidence_obj.get('mean_score', 0.0)
        
        metadata = {
            "file_name": path_obj.name,
            "file_size": path_obj.stat().st_size,
            "file_extension": path_obj.suffix,
            "file_path": file_path,
            "num_pages": getattr(docling_result.document, 'page_count', 0),
            "confidence_score": confidence_score,
        }

        # Add document-level metadata if available
        if hasattr(docling_result.document, 'metadata'):
            metadata['document_metadata'] = str(docling_result.document.metadata)
        
        return metadata

    def process_directory(self, directory_path: str) -> List[ProcessingResult]:
        """
        Process all PDFs in the directory
        """
        results = []
        dir_path = Path(directory_path)
        if not dir_path.exists() or not dir_path.is_dir():
            raise ValueError(f"Directory {directory_path} does not exist or is not a directory")

        # Iterate through all PDF files
        for pdf_file in dir_path.glob("*.pdf"):
            result = self.process(str(pdf_file))
            results.append(result)
        
        return results
    
    def get_config(self) -> DocumentConfig:
        """
        Get the configuration for the processor.
        """
        return self._config
    
    def update_config(self, config: DocumentConfig) -> None:
        """
        Update the configuration for the processor.
        """
        self._config = config
        self._converter = self._initialize_converter()

class DocumentProcessorFactory:
    """
    Factory for creating document processors.
    """
    @staticmethod
    def create_pdf_processor(config: Optional[DocumentConfig] = None) -> DoclingPDFProcessor:
        """
        Create a PDF document processor.
        """
        return DoclingPDFProcessor(config)

    @staticmethod
    def create_processor_with_defaults() -> DoclingPDFProcessor:
        """
        Create a PDF document processor with default configuration.
        OCR is disabled by default for lightweight processing.
        """
        default_config = DocumentConfig(
            extract_tables=True,
            extract_images=True,
            ocr_enabled=False,  # Disabled for lightweight processing
            force_ocr=False,
            pdf_backend="dlparse_v4",
            image_export_mode="embedded",
            table_mode="accurate",
            num_threads=4,
            timeout_per_document=300,
            accelerator_device="auto",
        )
        return DoclingPDFProcessor(default_config)

def docling_process(file_path: str, config: Optional[DocumentConfig] = None) -> ProcessingResult:
    """
    Simplified function API for a single document processing. 
    """
    processor = DocumentProcessorFactory.create_pdf_processor(config)
    return processor.process(file_path)

if __name__ == "__main__":
    import argparse
    
    # =========================================================================
    # CLI Argument Parsing
    # =========================================================================
    parser = argparse.ArgumentParser(
        description="Docling PDF Processor - Convert PDFs to Markdown and JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default paths (tests/assets → tests/output)
  python processor.py
  
  # Specify custom input and output directories
  python processor.py --input-dir /path/to/pdfs --output-dir /path/to/output
  
  # Process specific directory with default output
  python processor.py --input-dir ./my-pdfs
        """
    )
    parser.add_argument(
        "--input-dir", 
        help="Directory containing PDF files to process (default: tests/assets)",
        default=None
    )
    parser.add_argument(
        "--output-dir", 
        help="Directory for output files (default: tests/output)",
        default=None
    )
    args = parser.parse_args()
    
    # Set paths from CLI or defaults
    if args.input_dir:
        assets_dir = Path(args.input_dir)
    else:
        assets_dir = Path(__file__).parent.parent.parent / "tests" / "assets"
    
    if args.output_dir:
        output_base = Path(args.output_dir)
    else:
        output_base = Path(__file__).parent.parent.parent / "tests" / "output"
    
    print(f"\n📁 Input directory: {assets_dir}")
    print(f"📁 Output directory: {output_base}\n")
    
    # Get first PDF for single-file examples
    pdf_files = list(assets_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"❌ No PDF files found in {assets_dir}")
        exit(1)
    pdf_path = pdf_files[0]
    
    # =========================================================================
    # Example 1: Using the simple function API (Facade)
    # =========================================================================
    print("=" * 70)
    print("Example 1: Simple Function API")
    print("=" * 70)
    
    if pdf_path.exists():
        result = docling_process(str(pdf_path))
        print(result)
        print(f"Markdown content length: {len(result.content)} characters")
        print(f"JSON content length: {len(result.json_content)} characters")
        print(f"Metadata: {result.metadata}")
        
        # Save outputs for inspection
        output_dir_ex1 = output_base / "example1"
        output_dir_ex1.mkdir(parents=True, exist_ok=True)
        
        # Save markdown
        md_path = output_dir_ex1 / f"{pdf_path.stem}.md"
        md_path.write_text(result.content, encoding='utf-8')
        print(f"✅ Markdown saved to: {md_path}")
        
        # Save JSON
        json_path = output_dir_ex1 / f"{pdf_path.stem}.json"
        json_path.write_text(result.json_content, encoding='utf-8')
        print(f"✅ JSON saved to: {json_path}")
        
        # Save metadata
        metadata_path = output_dir_ex1 / f"{pdf_path.stem}_metadata.json"
        metadata_path.write_text(json.dumps(result.metadata, indent=2), encoding='utf-8')
        print(f"✅ Metadata saved to: {metadata_path}")
    else:
        print(f"PDF file not found at: {pdf_path}")
    
    # =========================================================================
    # Example 2: Function API with Custom Config
    # =========================================================================
    print("\n" + "=" * 70)
    print("Example 2: Function API with Custom Config (no images)")
    print("=" * 70)
    
    # Create custom configuration (OCR disabled, no images for lightweight processing)
    custom_config = DocumentConfig(
        extract_tables=True,
        extract_images=False,
        ocr_enabled=False
    )
    
    # Create processor using factory
    processor = DocumentProcessorFactory.create_pdf_processor(custom_config)
    
    # Process single file
    if pdf_path.exists():
        result = processor.process(str(pdf_path))
        print(f"Success: {result.success}")
        if result.success:
            print(f"Extracted {len(result.content)} markdown characters")
            print(f"Extracted {len(result.json_content)} JSON characters")
            print(f"Pages: {result.metadata.get('num_pages', 'N/A')}")
            print(f"Confidence: {result.metadata.get('confidence_score', 'N/A')}")
            
            # Save outputs for Example 2
            output_dir_ex2 = output_base / "example2"
            output_dir_ex2.mkdir(parents=True, exist_ok=True)
            
            md_path = output_dir_ex2 / f"{pdf_path.stem}_custom.md"
            md_path.write_text(result.content, encoding='utf-8')
            
            json_path = output_dir_ex2 / f"{pdf_path.stem}_custom.json"
            json_path.write_text(result.json_content, encoding='utf-8')
            
            metadata_path = output_dir_ex2 / f"{pdf_path.stem}_custom_metadata.json"
            metadata_path.write_text(json.dumps(result.metadata, indent=2), encoding='utf-8')
            
            print(f"✅ Outputs saved to: {output_dir_ex2}")
        else:
            print(f"Error: {result.error_message}")
    else:
        print(f"PDF file not found at: {pdf_path}")
    
    # =========================================================================
    # Example 3: Batch Processing (All PDFs in Directory)
    # =========================================================================
    print("\n" + "=" * 70)
    print("Example 3: Batch Processing (All PDFs in Directory)")
    print("=" * 70)
    
    # Create fresh processor for batch processing
    batch_processor = DocumentProcessorFactory.create_pdf_processor()
    
    # Process all PDFs in directory
    if assets_dir.exists():
        try:
            results = batch_processor.process_directory(str(assets_dir))
            print(f"Processed {len(results)} files")
            
            # Save output for each processed file
            output_dir_ex3 = output_base / "example3_batch"
            output_dir_ex3.mkdir(parents=True, exist_ok=True)
            
            for r in results:
                print(f"  - {r}")
                if r.success:
                    base_name = Path(r.file_path).stem
                    
                    # Save markdown
                    md_path = output_dir_ex3 / f"{base_name}.md"
                    md_path.write_text(r.content, encoding='utf-8')
                    
                    # Save JSON
                    json_path = output_dir_ex3 / f"{base_name}.json"
                    json_path.write_text(r.json_content, encoding='utf-8')
                    
                    # Save metadata
                    metadata_path = output_dir_ex3 / f"{base_name}_metadata.json"
                    metadata_path.write_text(json.dumps(r.metadata, indent=2), encoding='utf-8')
                    
                    print(f"    ✅ Saved: {base_name}.md, {base_name}.json, {base_name}_metadata.json")
            
            print(f"\n✅ All batch outputs saved to: {output_dir_ex3}")
        except ValueError as e:
            print(f"Directory processing error: {e}")
    else:
        print(f"Assets directory not found at: {assets_dir}")
    
    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"📁 All outputs saved to: {output_base}")
    print(f"   ├── example1/       (single file, full config)")
    print(f"   ├── example2/       (single file, custom config)")
    print(f"   └── example3_batch/ (all {len(pdf_files)} PDFs)")