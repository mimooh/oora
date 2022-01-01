oora.py is an oracle database cmdline client  
it is a wrapper around cx_Oracle python library  
it is an sqlplus replacement to produce better tabular results:

	name         ; value ; date                
	some name    ; 11    ; 2021-12-16 18:26:46 
	another      ; 121   ; 2021-03-25 20:08:22 
	and one more ; 0     ; 2021-03-25 20:08:31 


### CLIENT INSTALL

git clone https://github.com/mimooh/oora.git  
pip3 install cx_oracle prettytable  

in ~/.bashrc:  
export OORA_USER=user  
export OORA_PASS=pass  
export OORA_HOST=localhost

### FOR COMFORT

sudo ln -sf /path/to/oora.py /usr/local/bin/oora  
sudo chmod 775 /usr/local/bin/oora  

### USAGE

oora -c "select * from xyz"  
