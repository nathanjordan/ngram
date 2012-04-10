import sys
import random

from pymongo import Connection
from pymongo.code import Code

random.seed()

connection = Connection()
grams6 = connection.test.grams6
grams3 = connection.test.grams3

def findRandomWord():
	num = random.randint( 0 , grams6.count() - 1 )
	return grams6.find().limit(-1).skip(num).next()["word"]

startWord = findRandomWord()

k = startWord

text = ""

for i in range( 0 , 150 ):
	
	x = grams3.find( { "word0" : k } )
	
	while x.count() < 1:
	
		x = grams3.find( { "word0" : findRandomWord() } )
	
	num = random.randint( 0 , x.count() - 1 )
	
	y = x.next()
	
	for j in range( 0 , num - 1 ):
	
		y = x.next()
	
	text = text + " " + y["word0"] + " " + y["word1"]
	
	k = y["word2"]
	
print text
