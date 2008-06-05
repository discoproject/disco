
gcc -g -O3 -static -I ../ext/ -Wall -o ext_test ../ext/disco.c ext_test.c -lJudy

PYTHONPATH=../py python ext_test.py
