oora.py is an alternative to sqlplus (wraping, delimiter stuff)  
oora.py is a shell wrapper around cx_Oracle python library

# INSTALL

pip3 install cx_oracle  

in ~/.bashrc:  
export ORACLE_USER=user  
export ORACLE_PASS=pass  

# FOR COMFORT

sudo ln -sf oora.py /usr/local/bin/oora  
sudo chmod 775 /usr/local/bin/oora  

# USAGE
oora -q "select * from xyz"  
oora -d '|' -q "select * from xyz"   
