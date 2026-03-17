"""
PySpark + Docling Integration
====================================
Process PDFs in parallel using PySpark

Think of it like this:
1. You have a list of PDF files
2. PySpark splits them across many workers
3. Each worker processes their PDFs using docling_process()
4. Results come back to you
"""

# ============================================================================
# Step 1: Import the tools we need
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_json
from pyspark.sql.types import (
    StructType,      # Like a template for a form
    StructField,     # Like a field on the form
    BooleanType,     # True/False
    StringType,      # Text
    MapType          # Dictionary/Key-Value pairs
)
import sys
import argparse
from pathlib import Path

# Add our code to Python's search path
sys.path.insert(0, str(Path(__file__).parent))

# ============================================================================
# Step 2: Define what the result looks like (Schema)
# ============================================================================
def get_result_schema() -> StructType:
    """
    This tells PySpark what our result looks like.
    
    Think of it like a form template:
    - Checkbox: Did it succeed? (True/False)
    - Text box: What's the content? (Text - Markdown format)
    - Text box: JSON content (Text - Docling JSON format for docling serve)
    - Dictionary: Extra information (Key-Value pairs)
    - Text box: Error message if failed (Text)
    - Text box: Which file was it? (Text)
    """
    return StructType([
        StructField("success", BooleanType(), nullable=False),        # Required
        StructField("content", StringType(), nullable=True),          # Optional (Markdown)
        StructField("json_content", StringType(), nullable=True),     # Optional (JSON for docling serve)
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),  # Optional
        StructField("error_message", StringType(), nullable=True),    # Optional
        StructField("file_path", StringType(), nullable=False),       # Required
    ])

# ============================================================================
# Step 3: Wrap our function for PySpark
# ============================================================================
def process_pdf_wrapper(file_path: str) -> dict:
    """
    This is a wrapper around docling_process that:
    1. Calls docling_process(file_path)
    2. Gets the result
    3. Converts it to a dictionary
    4. Makes sure all metadata values are strings (PySpark requirement)
    5. Returns the dictionary
    
    Why convert metadata values to strings?
    Because PySpark's MapType needs all values to be the same type!
    """
    # Import inside the function (lazy import for Spark workers)
    from docling_module.processor import docling_process

    # Call the docling_process function
    # This creates a NEW processor on each worker (not serialized from driver)
    result = docling_process(file_path)
    
    # Convert the result to a dictionary
    result_dict = result.to_dict()
    
    # Convert all metadata values to strings (PySpark requirement)
    if result_dict.get('metadata'):
        result_dict['metadata'] = {
            key: str(value) if value is not None else ""
            for key, value in result_dict['metadata'].items()
        }
    else:
        result_dict['metadata'] = {}
    
    return result_dict

# ============================================================================
# Step 4: Create a Spark session (The Teacher)
# ============================================================================
def create_spark():
    """
    Create the Spark "Driver" that manages everything.
    
    When running in Spark Operator, master and resources are controlled by K8s.
    """
    print("*" * 70)
    print("Creating Spark session...")
    print("*" * 70)
    
    spark = SparkSession.builder \
        .appName("DoclingSparkJob") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .getOrCreate()

    # make it less chatty
    spark.sparkContext.setLogLevel("WARN")

    # Distribute the docling_module to workers as a zip file
    import os
    import zipfile
    import tempfile
    
    module_path = os.path.join(os.path.dirname(__file__), "docling_module")
    
    if os.path.exists(module_path):
        # Create a temporary zip file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
            zip_path = tmp.name
        
        # Package the entire docling_module directory
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(module_path):
                for file in files:
                    if file.endswith('.py'):
                        file_path = os.path.join(root, file)
                        # Create archive name preserving package structure
                        arcname = os.path.relpath(file_path, os.path.dirname(module_path))
                        zipf.write(file_path, arcname)
                        print(f"Packaged: {arcname}")
        
        # Add the zip to Spark workers
        spark.sparkContext.addPyFile(zip_path)
        print(f"✅ Added docling_module package to Spark workers")
    else:
        print(f"⚠️ Warning: docling_module not found at {module_path}")

    print(f"Spark session created with {spark.sparkContext.defaultParallelism} workers")
    return spark

# ============================================================================
# Step 5: Main Function - Put it all together
# ============================================================================
def main():
    """
    The main function that does everything step by step.
    """
    parser = argparse.ArgumentParser(description="Docling Spark Job")
    parser.add_argument("--input-dir", help="Directory containing input PDFs", default=None)
    parser.add_argument("--output-dir", help="Directory for output files (markdown and jsonl per PDF)", default=None)
    # Keep legacy --output-file for backward compatibility
    parser.add_argument("--output-file", help="(Legacy) Path to combined output JSONL file", default=None)
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("📄 ENHANCED PDF PROCESSING WITH PYSPARK + DOCLING")
    print("="*70)
    
    # ========== STEP 1: Create Spark ==========
    spark = create_spark()
    
    # Define output_dir (for separate files per PDF)
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent.parent / "output"
    
    # Legacy support: if --output-file is specified, extract directory from it
    if args.output_file:
        legacy_output_path = Path(args.output_file)
        output_dir = legacy_output_path.parent
    
    try:
        # ========== STEP 2: Get list of PDF files ==========
        print("\n📂 Step 1: Getting list of PDF files...")
        
        if args.input_dir:
            assets_dir = Path(args.input_dir)
        else:
            assets_dir = Path(__file__).parent.parent / "assets"
        
        print(f"   Looking for PDFs in: {assets_dir}")
        
        if not assets_dir.exists():
             print(f"❌ Input directory not found: {assets_dir}")
             return

        # Create a list of file paths to process
        file_list = []
        
        # Find all PDFs in directory
        for pdf_file in assets_dir.glob("*.pdf"):
             file_list.append((str(pdf_file),))
             print(f"✅ Found PDF: {pdf_file.name}")
        
        if not file_list:
            print(f"❌ No PDF files found in {assets_dir}!")
            return
        
        # Create a DataFrame (like an Excel table)
        df_paths = spark.createDataFrame(file_list, ["document_path"])
        
        print(f"   Found {df_paths.count()} files to process")
        print("\n   Files:")
        df_paths.show(truncate=False)
        
        # ========== STEP 3: Register the UDF ==========
        print("\n⚙️  Step 2: Registering the processing function...")
        
        # Create the UDF (User Defined Function)
        # This wraps our process_pdf_wrapper so PySpark can use it
        docling_udf = udf(
            process_pdf_wrapper,        # Our wrapper function
            get_result_schema()         # What it returns
        )
        
        print("   ✅ Function registered")
        
        # ========== STEP 4: Process the files ==========
        print("\n🔄 Step 3: Processing files (this is where the magic happens!)...")
        print("   Spark is now distributing work to workers...")
        print("   Each worker will:")
        print("   - Import the enhanced docling processor")
        print("   - Process PDFs with modern DoclingParseV4DocumentBackend")
        print("   - Extract text, tables, and metadata")
        print("   - Return structured results")
        
        # Apply the UDF to each row
        # PySpark automatically splits this across workers!
        df_with_results = df_paths.withColumn(
            "result",                      # New column name
            docling_udf(col("document_path"))  # Apply function to each path
        )
        
        # ========== STEP 5: Flatten the results ==========
        print("\n📊 Step 4: Organizing results...")
        
        # Break apart the result into separate columns
        df_final = df_with_results.select(
            col("document_path"),
            col("result.success").alias("success"),
            col("result.content").alias("content"),           # Markdown content
            col("result.json_content").alias("json_content"), # JSON content for docling serve
            to_json(col("result.metadata")).alias("metadata"), # <--- Convert Map to JSON String
            col("result.error_message").alias("error_message")
        ).cache()
        
        # Force computation to cache it
        count = df_final.count()
        
        # ========== STEP 6: Show the results ==========
        print(f"\n✅ Step 5: Results are ready! (Count: {count})\n")
        
        print("📋 What the data looks like:")
        df_final.printSchema()
        
        print("\n📊 The results:")
        df_final.show(truncate=50)

        # ========== STEP 6.5: Show full error messages for failed documents ==========
        if df_final.filter(col("success") == False).count() > 0:
            print("\n🔍 Full error messages for failed documents:")
            failed_docs = df_final.filter(col("success") == False).select("document_path", "error_message")
            for row in failed_docs.collect():
                print(f"\n📄 {row['document_path']}:")
                print(f"Error: {row['error_message']}")

        # ========== STEP 7: Analyze results ==========
        print("\n📈 Analysis:")
        
        total = df_final.count()
        successful = df_final.filter(col("success") == True).count()
        failed = df_final.filter(col("success") == False).count()
        
        print(f"Total files: {total}")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        
        # Skip detailed display to avoid memory/serialization issues
        print("\n💡 Skipping detailed result display to prevent worker crashes.")
        print("Full results will be saved to the output file...")
        
        # ========== STEP 8: Save results - SEPARATE FILES PER PDF ==========
        print("\n💾 Step 6: Saving results (separate files per PDF)...")

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Output directory: {output_dir}")
        print("Collecting data to driver to write locally...")
        
        # Convert to Pandas
        pdf_df = df_final.toPandas()
        
        # Save separate files for each PDF
        import json as json_lib
        for idx, row in pdf_df.iterrows():
            # Get base filename from the original PDF path
            original_path = Path(row['document_path'])
            base_name = original_path.stem  # filename without extension
            
            if row['success']:
                # 1. Save Markdown file
                markdown_path = output_dir / f"{base_name}.md"
                with open(markdown_path, 'w', encoding='utf-8') as f:
                    f.write(row['content'] if row['content'] else "")
                print(f"   ✅ Markdown saved: {markdown_path.name}")
                
                # 2. Save JSONL file (docling format for docling serve)
                jsonl_path = output_dir / f"{base_name}.json"
                with open(jsonl_path, 'w', encoding='utf-8') as f:
                    f.write(row['json_content'] if row['json_content'] else "{}")
                print(f"   ✅ JSON saved: {jsonl_path.name}")
                
                # 3. Save metadata as separate file
                metadata_path = output_dir / f"{base_name}_metadata.json"
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    # metadata is already a JSON string from to_json()
                    metadata = json_lib.loads(row['metadata']) if row['metadata'] else {}
                    metadata['source_file'] = str(original_path)
                    metadata['success'] = True
                    json_lib.dump(metadata, f, indent=2, ensure_ascii=False)
                print(f"   ✅ Metadata saved: {metadata_path.name}")
            else:
                # Save error information for failed files
                error_path = output_dir / f"{base_name}_error.json"
                with open(error_path, 'w', encoding='utf-8') as f:
                    error_info = {
                        'source_file': str(original_path),
                        'success': False,
                        'error_message': row['error_message']
                    }
                    json_lib.dump(error_info, f, indent=2, ensure_ascii=False)
                print(f"   ❌ Error saved: {error_path.name}")
        
        # Also save a combined summary JSONL for reference
        summary_path = output_dir / "summary.jsonl"
        summary_df = pdf_df[['document_path', 'success', 'metadata', 'error_message']].copy()
        summary_df.to_json(summary_path, orient='records', lines=True, force_ascii=False)
        print(f"\n   📋 Summary saved: {summary_path}")
        
        print(f"\n✅ All results saved to: {output_dir}")
        print("\n🎉 ALL DONE!")
        print("✅ Enhanced processing complete!")
        print("\n📥 To download results from PVC:")
        print("   ./k8s/deploy.sh download ./output/")

        if failed > 0:
            print(f"\n⚠️  {failed}/{total} documents failed processing.")
            if successful == 0:
                print("❌ All documents failed! Exiting with error.")
                sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        print("\n🛑 Stopping Spark...")
        spark.stop()
        print("✅ Bye!")

if __name__ == "__main__":
    main()