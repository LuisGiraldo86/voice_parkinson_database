import json
import os
from pymongo import MongoClient
from jsonschema import validate, ValidationError

# Define schema (adapt as needed)
study_schema = {
    "type": "object",
    "properties": {
        "study_id": {"type": "string"},
        "title": {"type": "string"},
        "authors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "affiliations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "institution": {"type": "string"},
                                "country": {"type": "string"}
                            },
                            "required": ["institution", "country"]
                        }
                    }
                },
                "required": ["name"]
            }
        },
        "ml_problem_type": {"type": "array", "items": {"type": "string"}},
        "problem": {"type": "string"},
        "publication_type": {"type": "string"},
        "journal": {"type": "string"},
        "year": {"type": ["integer", "string"]},  # allow 2020 or "2020"
        "doi": {"type": "string"},
        "abstract": {"type": "string"},

        "source_dataset": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "size": {"type": ["integer", "string", "null"]},
                    "pd_patients": {"type": ["integer", "null"]},
                    "controls": {"type": ["integer", "null"]},
                    "url": {"type": ["string", "null"]},
                    "doi": {"type": ["string", "null"]}
                },
                "required": ["name"]
            }
        },
        "target_dataset": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "size": {"type": ["integer", "string", "null"]},
                    "pd_patients": {"type": ["integer", "null"]},
                    "controls": {"type": ["integer", "null"]},
                    "url": {"type": ["string", "null"]},
                    "doi": {"type": ["string", "null"]}
                },
                "required": ["name"]
            }
        },

        "ml_approaches": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "algorithm": {"type": "string"},
                    "framework": {"type": ["string", "null"]},
                    "feature_extraction": {
                        "type": "array", 
                        "items": {"type": "string"}
                    },
                    "feature_selection": {
                        "oneOf": [
                            {"type": "null"},
                            {
                                "type": "object",
                                "properties": {
                                    "methods": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "combination": {"type": "string"}
                                },
                                "required": ["methods"]
                            }
                        ]
                    },
                    "missing_value_handling": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "outlier_handling": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "imbalance_handling": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "scaling": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "dimensionality_reduction": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "sample_selection": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "input_speech": {"type": "string"},
                    "validation": {"type": ["string", "null"]},
                    "results": {
                        "type": "object",
                        "patternProperties": {
                            ".*": {  # any key is allowed
                                "type": ["number", "string", "null"]
                            }
                        },
                        "additionalProperties": False
                    }
                },
                "required": ["algorithm", "results"]
            }
        }
    },
    "required": ["study_id", "title", "authors", "publication_type", "journal", "year", "doi", "ml_approaches"],
    "additionalProperties": True  # Allow for future extensibility
}


def import_json_studies(
    mongo_uri="mongodb://localhost:27017/",
    db_name="pd_review",
    collection_name="studies",
    skip_duplicate_check=False
):
    folder_path = input("Enter the path to the folder containing study JSON files: ").strip()

    if not os.path.isdir(folder_path):
        print(f"❌ The folder '{folder_path}' does not exist.")
        return
    
    # Ask about duplicate checking
    print("\n🔍 Duplicate Detection Options:")
    print("1. Skip duplicate detection (import all files)")
    print("2. Enhanced duplicate detection (same DOI/title + same dataset)")
    print("3. Basic duplicate detection (same DOI/title only)")
    
    dup_choice = input("Choose duplicate detection method (1-3): ").strip()
    
    if dup_choice == "1":
        skip_duplicate_check = True
        print("✅ Duplicate detection disabled - all files will be imported")
    elif dup_choice == "2":
        skip_duplicate_check = False
        print("✅ Enhanced duplicate detection enabled")
    else:
        skip_duplicate_check = "basic"
        print("✅ Basic duplicate detection enabled")

    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    count_inserted, count_skipped, count_invalid, count_load_errors = 0, 0, 0, 0
    duplicate_report, validation_report, load_error_report = [], [], []

    # Walk through all subfolders
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith(".json"):
                file_path = os.path.join(root, filename)
                with open(file_path, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)

                        # ✅ Schema validation
                        try:
                            validate(instance=data, schema=study_schema)
                        except ValidationError as e:
                            validation_report.append({
                                "file": file_path,
                                "error": str(e)
                            })
                            print(f"❌ Schema validation failed: {file_path}")
                            count_invalid += 1
                            continue

                        # ✅ Duplicate check (if enabled)
                        if not skip_duplicate_check:
                            query = {}
                            
                            if skip_duplicate_check == "basic":
                                # Basic duplicate detection (original logic)
                                if "doi" in data and data["doi"]:
                                    query["doi"] = data["doi"]
                                elif "title" in data:
                                    query["title"] = data["title"]
                                    
                                reason_text = "doi" if "doi" in query else "title"
                                duplicate_text = f"⚠️ Duplicate found (same {reason_text}): {file_path}"
                                
                            else:
                                # Enhanced duplicate detection - include dataset information
                                if "doi" in data and data["doi"]:
                                    # Check for same DOI AND same dataset names
                                    dataset_names = []
                                    if "dataset" in data and isinstance(data["dataset"], list):
                                        dataset_names = [d.get("name", "") for d in data["dataset"] if isinstance(d, dict)]
                                    
                                    query = {
                                        "doi": data["doi"],
                                        "dataset.name": {"$in": dataset_names} if dataset_names else {"$exists": True}
                                    }
                                elif "title" in data:
                                    # Check for same title AND same dataset names
                                    dataset_names = []
                                    if "dataset" in data and isinstance(data["dataset"], list):
                                        dataset_names = [d.get("name", "") for d in data["dataset"] if isinstance(d, dict)]
                                    
                                    query = {
                                        "title": data["title"],
                                        "dataset.name": {"$in": dataset_names} if dataset_names else {"$exists": True}
                                    }
                                
                                reason_text = "doi+dataset" if "doi" in query else "title+dataset"
                                duplicate_text = f"⚠️ Duplicate found (same DOI/title + dataset): {file_path}"

                            if query and (existing := collection.find_one(query)):
                                duplicate_report.append({
                                    "file": file_path,
                                    "reason": reason_text,
                                    "value": data.get("doi", data.get("title", "unknown")),
                                    "existing_id": str(existing["_id"]),
                                })
                                print(duplicate_text)
                                count_skipped += 1
                                continue

                        # ✅ Insert if valid + not duplicate
                        collection.insert_one(data)
                        count_inserted += 1
                        
                    except json.JSONDecodeError as e:
                        load_error_report.append({
                            "file": file_path,
                            "error_type": "JSON Decode Error",
                            "error_message": str(e),
                            "line": getattr(e, 'lineno', 'unknown'),
                            "column": getattr(e, 'colno', 'unknown')
                        })
                        print(f"❌ JSON decode error in {file_path}: {e}")
                        count_load_errors += 1
                        
                    except Exception as e:
                        load_error_report.append({
                            "file": file_path,
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        })
                        print(f"❌ Unexpected error in {file_path}: {e}")
                        count_load_errors += 1

    # 📊 Final summary
    print(f"\n✅ Imported {count_inserted} studies")
    print(f"⚠️ Skipped {count_skipped} duplicates")
    print(f"❌ Rejected {count_invalid} invalid files")
    print(f"💥 Failed to load {count_load_errors} files")

    # 📄 Save reports
    if duplicate_report:
        with open("duplicate_report.json", "w", encoding="utf-8") as f:
            json.dump(duplicate_report, f, indent=2)
        print("📄 Duplicate report saved to duplicate_report.json")

    if validation_report:
        with open("validation_report.json", "w", encoding="utf-8") as f:
            json.dump(validation_report, f, indent=2)
        print("📄 Validation report saved to validation_report.json")

    if load_error_report:
        with open("load_error_report.json", "w", encoding="utf-8") as f:
            json.dump(load_error_report, f, indent=2)
        print("📄 Load error report saved to load_error_report.json")
        print("🔍 Check load_error_report.json for files that couldn't be loaded")

def check_file_health(folder_path=None):
    """
    Utility function to check all JSON files for basic loading issues.
    Useful for identifying problematic files before attempting import.
    """
    if not folder_path:
        folder_path = input("Enter the path to check JSON files: ").strip()
    
    if not os.path.isdir(folder_path):
        print(f"❌ The folder '{folder_path}' does not exist.")
        return
    
    print(f"🔍 Checking JSON files in: {folder_path}")
    
    total_files = 0
    valid_files = 0
    problematic_files = []
    
    # Walk through all subfolders
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith(".json"):
                file_path = os.path.join(root, filename)
                total_files += 1
                
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        
                    # Basic validation - check if it has required fields
                    missing_fields = []
                    for required_field in ["title", "year", "doi", "ml_approaches"]:
                        if required_field not in data:
                            missing_fields.append(required_field)
                    
                    if missing_fields:
                        problematic_files.append({
                            "file": file_path,
                            "issue": "Missing required fields",
                            "details": missing_fields
                        })
                    else:
                        valid_files += 1
                        
                except json.JSONDecodeError as e:
                    problematic_files.append({
                        "file": file_path,
                        "issue": "JSON Decode Error",
                        "details": f"Line {getattr(e, 'lineno', '?')}, Column {getattr(e, 'colno', '?')}: {str(e)}"
                    })
                except Exception as e:
                    problematic_files.append({
                        "file": file_path,
                        "issue": type(e).__name__,
                        "details": str(e)
                    })
    
    # Summary
    print(f"\n📊 File Health Check Summary:")
    print(f"  📄 Total JSON files: {total_files}")
    print(f"  ✅ Valid files: {valid_files}")
    print(f"  ❌ Problematic files: {len(problematic_files)}")
    
    if problematic_files:
        print(f"\n⚠️ Issues found in {len(problematic_files)} files:")
        for issue in problematic_files:
            print(f"  📄 {issue['file']}")
            print(f"     🔸 Issue: {issue['issue']}")
            print(f"     🔸 Details: {issue['details']}")
            print()
        
        # Save detailed report
        with open("file_health_report.json", "w", encoding="utf-8") as f:
            json.dump(problematic_files, f, indent=2)
        print("📄 Detailed report saved to file_health_report.json")
    else:
        print("🎉 All files are healthy and ready for import!")

def main():
    print("📋 Review Database Import Tool")
    print("1. Import studies to MongoDB")
    print("2. Check file health")
    
    choice = input("\nChoose an option (1 or 2): ").strip()
    
    if choice == "1":
        import_json_studies()
    elif choice == "2":
        check_file_health()
    else:
        print("❌ Invalid choice. Please run again and choose 1 or 2.")

if __name__ == "__main__":
    main()
