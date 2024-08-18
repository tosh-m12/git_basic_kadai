import json
with open("temp.json", "r") as f:
    data = json.load(f)


file_path = data["file_path"]
sheet_name = data["sheet_name"]
search_text = data["search_text"]
ac_term = data["ac_term"]
bill_from = data["bill_from"]
bill_to = data["bill_to"]
b_cat = data["b_cat"]
mxbd_rcvd = data["mxbd_rcvd"]
csv_output_path = data["csv_output_path"]
rate = data["rate"]
rate_date = data["rate_date"]

print(file_path,sheet_name,search_text,ac_term,bill_from,bill_to,b_cat,mxbd_rcvd,csv_output_path, rate, rate_date)