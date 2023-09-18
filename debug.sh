cd submit
rm -rf ./*
mkdir lindorm-tsdb-contest-cpp
cp -r ../source lindorm-tsdb-contest-cpp/
cp -r ../include lindorm-tsdb-contest-cpp/
cp ../ccflags .
touch debug
zip -r lindorm-tsdb-contest-cpp.zip lindorm-tsdb-contest-cpp debug ccflags