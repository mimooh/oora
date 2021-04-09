oora.py is an oracle database cmdline client  
it is an alternative to sqlplus (wraping, delimiter stuff)  
it is a shell wrapper around cx_Oracle python library  
it is meant for comfort, less for speed

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
