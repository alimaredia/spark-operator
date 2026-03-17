# Docling Module

A Python module for converting PDF documents to Markdown and JSON using [Docling](https://github.com/DS4SD/docling). This module provides a clean, object-oriented API for document processing with support for tables, images, and optional OCR.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration Options](#configuration-options)
- [Output Structure](#output-structure)
- [Integration with Spark](#integration-with-spark)

## Overview

The `docling_module` is a standalone document processing package that:

- Converts PDF documents to **Markdown** (human-readable) and **JSON** (machine-readable)
- Extracts tables with accurate structure preservation
- Handles embedded images with configurable export modes
- Supports multiple OCR engines for scanned documents
- Provides both simple function API and full object-oriented interface
- Works standalone or integrated with PySpark for distributed processing

## Architecture

```
docling_module/
├── __init__.py          # Package exports
├── processor.py         # Core processing logic
└── README.md            # This documentation
```

## Installation

### Prerequisites

```bash
# Core dependencies
pip install docling docling-core

# Optional: OCR engines (if OCR is needed)
pip install rapidocr-onnxruntime  # Lightweight, pure Python
pip install pytesseract           # CLI wrapper for tesseract
pip install tesserocr             # Fast C-binding (requires libtesseract-dev)
```

### System Dependencies (for tesserocr)

```bash
# Ubuntu/Debian
sudo apt-get install tesseract-ocr libtesseract-dev leptonica-dev

# macOS
brew install tesseract
```

## Quick Start

### Step 1: Navigate to the Project Directory

```bash
# Clone the repo (if not already done)
git clone https://github.com/opendatahub-io/spark-operator.git
cd spark-operator/examples/openshift
```

### Step 2: Set Up Virtual Environment

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Run the Processor

**Option A: Use sample PDFs (quickest way to test)**

```bash
python scripts/docling_module/processor.py
```

This will:
- Process all PDFs in `tests/assets/`
- Save outputs to `tests/output/`
- Run 3 examples (single file, custom config, batch processing)

**OR**

**Option B: Use your own PDFs**

```bash
# Process your own PDFs with custom input/output directories
python scripts/docling_module/processor.py \
    --input-dir /path/to/your/pdfs \
    --output-dir /path/to/save/output

# Example with relative paths
python scripts/docling_module/processor.py \
    --input-dir ./my-pdfs \
    --output-dir ./my-output
```

### Step 4: Check the Output

```bash
# View generated files
ls -la tests/output/

# View markdown content
cat tests/output/example1/*.md

# View the output structure
find tests/output -type f | head -20
```

### Using as a Python Module

```python
# Option 1: Simple function API
from scripts.docling_module import docling_process

result = docling_process("document.pdf")
if result.success:
    print(result.content)      # Markdown content
    print(result.json_content) # JSON content

# Option 2: Batch processing
from scripts.docling_module import DocumentProcessorFactory

processor = DocumentProcessorFactory.create_processor_with_defaults()
results = processor.process_directory("/path/to/pdfs")

for r in results:
    if r.success:
        print(f"✅ {r.file_path}: {len(r.content)} chars")
```

## Configuration Options

### DocumentConfig Parameters

```python
from docling_module import DocumentConfig, DocumentProcessorFactory

config = DocumentConfig(
    # === Basic Options ===
    extract_tables=True,       # Extract tables from PDF
    extract_images=True,       # Extract/embed images
    ocr_enabled=False,         # Enable OCR for scanned docs
    max_pages=None,            # Limit pages (None = all)
    
    # === Advanced Options ===
    pdf_backend="dlparse_v4",  # PDF parsing backend
    image_export_mode="embedded",  # "embedded" | "placeholder" | "referenced"
    table_mode="accurate",     # Table extraction mode
    num_threads=4,             # Processing threads
    timeout_per_document=300,  # Timeout in seconds
    
    # === OCR Options (when ocr_enabled=True) ===
    ocr_engine="rapidocr",     # "rapidocr" | "tesserocr" | "tesseract"
    force_ocr=False,           # Force OCR even on digital PDFs
    
    # === Enrichment Options ===
    enrich_code=False,         # Detect and format code blocks
    enrich_formula=False,      # Detect and format formulas
    enrich_picture_classes=False,    # Classify images
    enrich_picture_description=False, # Generate image descriptions
    
    # === Performance ===
    accelerator_device="auto"  # "auto" | "cpu" | "gpu"
)

processor = DocumentProcessorFactory.create_pdf_processor(config)
```

## Output Structure

### ProcessingResult Object

| Field | Type | Description |
|-------|------|-------------|
| `success` | `bool` | Whether processing succeeded |
| `content` | `str` | Markdown content |
| `json_content` | `str` | JSON string (docling format) |
| `metadata` | `Dict` | File and document metadata |
| `error_message` | `Optional[str]` | Error details if failed |
| `file_path` | `str` | Original file path |

### Metadata Fields

```python
{
    "file_name": "document.pdf",
    "file_size": 1234567,
    "file_extension": ".pdf",
    "file_path": "/path/to/document.pdf",
    "num_pages": 10,
    "confidence_score": 0.95,
    "document_metadata": "..."  # If available
}
```

### File Output Structure

When using CLI or batch processing:

```
tests/output/
├── example1/                    # Single file, default config
│   ├── document.md              # Markdown content
│   ├── document.json            # JSON content (docling format)
│   └── document_metadata.json   # Processing metadata
│
├── example2/                    # Single file, custom config
│   ├── document_custom.md
│   ├── document_custom.json
│   └── document_custom_metadata.json
│
└── example3_batch/              # All PDFs in directory
    ├── paper1.md
    ├── paper1.json
    ├── paper1_metadata.json
    ├── paper2.md
    ├── paper2.json
    ├── paper2_metadata.json
    └── ...
```

## Integration with Spark

This module is designed to work with PySpark for distributed processing. The `run_spark_job.py` script uses this module internally.

### How It Works

1. **Spark Job** distributes PDF files across workers
2. Each worker uses `docling_module` to process its assigned PDFs
3. Results are collected and saved (markdown, JSON, metadata per PDF)

### Architecture

```
run_spark_job.py
    │
    ├── Creates Spark session
    ├── Reads PDF paths from input directory
    ├── Distributes to workers via UDF
    │       │
    │       └── Each worker calls:
    │               docling_module.docling_process(pdf_path)
    │
    └── Collects results and saves outputs
```