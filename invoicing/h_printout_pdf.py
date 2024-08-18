import win32print
import win32api
import os

# Root directory containing PDF files
root_dir = r'./invoicing/data/202406'

# Get the printer name
printer_name = 'FX DocuCentre-IV 7080 PCL 6'

# Set the printer as default
try:
    hprinter = win32print.OpenPrinter(printer_name)
    win32print.SetDefaultPrinter(printer_name)
    win32print.ClosePrinter(hprinter)
except Exception as e:
    print(f"Failed to set the printer: {e}")
    exit(1)

# Walk through the directory tree
for dirpath, dirnames, filenames in os.walk(root_dir):
    for filename in filenames:
        if filename.lower().endswith('.pdf'):
            file_path = os.path.join(dirpath, filename)
            try:
                win32api.ShellExecute(
                    0,
                    "print",
                    file_path,
                    None,
                    ".",
                    0
                )
                print(f"Printing {file_path} to the printer: {printer_name}")
            except Exception as e:
                print(f"Failed to print the file: {file_path}, Error: {e}")
