The main command line client for Oracle database, sqlplus with it's wraping and lack of delimiter doesn't provide enough comfort.
This oora.py client is a shell wrapper around cx_Oracle python library

# INSTALL

pip3 install cx_oracle 

in ~/.bashrc:
export ORACLE_USER=user
export ORACLE_PASS=pass

# FOR COMFORT

sudo ln -sf oora.py /usr/local/bin/oora
sudo chmod 775 /usr/local/bin/oora
oora -q "select * from xyz"

