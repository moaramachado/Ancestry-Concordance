# Ancestry-Concordance
This code is used to perform concordance analysis on ancestry data for equipment/reagent verification.

### Pre-requisites:
* am_tools
* [numpy](https://numpy.org/)
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
* [Pandas](https://pandas.pydata.org/)
* [scipy](https://scipy.org/)
* [openpyxl](https://openpyxl.readthedocs.io/en/stable/)
* [xlsxwriter](https://xlsxwriter.readthedocs.io/)

### Input:
* **Original Files:** Json files with ancestry information for original samples.
* **Validation Files:** Json files with ancestry information for validation samples.

### Output:
* **Intermediate Files:** CSV files with the percentage of the ancestries inferred.
* **Result file:** Results of the concordance analysis for the validation and original samples in excel format.

### How to run it as a module from the am_tools:
```
am_tools ancestry germline-prod germline-test 8060256017 8060256248 /Users/moara.machado/Documents/Sema4/Projects/AM_tickets/AM-1271

```

### How to run it using the input example files
```
python scripts/ancestry_concordance.py pathway_for_the_input_folder

Ex: python scripts/ancestry_concordance.py /Users/moara.machado/Documents/Github_projects/Ancestry-Concordance
```
