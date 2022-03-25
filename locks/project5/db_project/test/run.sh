rm a.out
rm insert100find200Test
# cp insert100find200Testcp insert100find200Test

g++ -rdynamic -g -fno-stack-protector test.cc && ./a.out
