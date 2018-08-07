make clean
rm ../caso-1.0.0.tar.gz
rm -rf ../caso-1.0.0
mkdir ../caso-1.0.0
cp *.c *.h Makefile readme.pdf ../caso-1.0.0/
cp -r Jerasure/ ../caso-1.0.0/
mkdir ../caso-1.0.0/example-trace
cp ../msr-traces/wdev_3.csv ../msr-traces/wdev_1.csv ../caso-1.0.0/example-trace/
tar -czvf ../caso-1.0.0.tar.gz ../caso-1.0.0/ 
