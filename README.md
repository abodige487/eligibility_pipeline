Healthcare Eligibility Pipeline (PySpark)

Project File Structure

eligibility_pipeline/
pipeline.py - Main pipeline orchestration logic
transforms.py - Reusable transformations and validations
config.py - Partner-specific configuration (only file that changes)
data/ - Input eligibility files from partners
output/ - Final unified output file
README.md

Key idea:
pipeline.py and transforms.py contain generic logic
config.py contains all partner-specific details
