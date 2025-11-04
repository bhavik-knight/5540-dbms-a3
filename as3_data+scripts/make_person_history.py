# -*- coding: utf-8 -*-

import random

npids = 20000   # assume there are 20000 persons

print ('\n==================================================\n-')

num=input('How many records to generate? ')
try:
    num=int(num)
except:
    print('Invalid number. Bye!')
    exit(1)

try:
    inpf1 = open('cities.txt',"r")
except IOError:
    print ("\nError opening file cities.txt!")
    ch = input("Press <enter> to quit program...")
    exit(1)

try:
    inpf2 = open('countries.txt',"r")
except IOError:
    print ("\nError opening file countries.txt!")
    ch = input("Press <enter> to quit program...")
    exit(1)

C1 = inpf1.readlines()
C2 = inpf2.readlines()

outf=open('phistory.tsv', "w")

for i in range(num):
    pid   = random.randint(1, npids)
    year  = random.randint(1990, 2016)	
    month = random.randint(1, 12)
    j     = random.randint(1, len(C1))	
    city  = C1[j-1].strip('\n')
    j     = random.randint(1, len(C2))	
    country = C2[j-1]
    rec =  str(pid) + '\t' + str(year) + '\t' + str(month) + '\t' + city+ '\t' + country 
    outf.write(rec)

inpf1.close()
inpf2.close()
outf.close()

print ('\n-\n==================================================\n')


