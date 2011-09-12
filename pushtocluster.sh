for addr in "$@"
do
    scp -r txpete/ $addr:~
    ssh $addr "cd ~/txpete/; ./setup.sh"
done