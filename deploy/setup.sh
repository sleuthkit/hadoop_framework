sudo apt-get install unzip
sudo unzip  -n -qq sleuthkit-pipeline-1-SNAPSHOT-libs.zip -d /usr/lib/hadoop/lib/
for SERVICE in /etc/init.d/hadoop*
do
    sudo $SERVICE restart
done
echo "export LD_LIBRARY_PATH=$HOME/txpete/fsrip/deps/" >> $HOME/.bashrc
echo "export PATH=$PATH:$HOME/txpete/fsrip/" >> $HOME/.bashrc
echo "export HADOO_HOME=/usr/lib/hadoop/" >> $HOME/.bashrc