import subprocess



subprocess.run(["python", "./Forvia_mailsys/invoicing/v_drop_bd_mexico.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/v_drop_dnmasterlist.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/v_drop_f_extract.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/u_create_bd_mexico.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/u_create_dnmasterlist.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/u_create_f_extract.py"])

