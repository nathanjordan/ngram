import sys
import random

from pymongo import Connection
from pymongo.code import Code

random.seed()

connection = Connection()
grams6 = connection.test.grams6
grams7 = connection.test.grams7
grams3 = connection.test.grams3
grams8 = connection.test.grams8
grams9 = connection.test.grams9
grams10 = connection.test.grams10

def findRandomWord():
	num = random.randint( 0 , grams6.count() - 1 )
	return grams6.find().limit(-1).skip(num).next()["word"]

startWord = findRandomWord()

k = startWord

text = ""

for i in range( 0 , 200 ):
	
	print "generate gram " + str(i)
	
	#####################################################################
	####    First Word
	#####################################################################
	grams7.drop()
	grams8.drop()
	grams9.drop()
	grams10.drop()
	
	firstWord = k
	
	#find grams with same first word
	x = grams3.find( { "word0" : k } )
	
	#if there are none, find a random word
	while x.count() < 1:
		
		firstWord = findRandomWord()
		
		x = grams3.find( { "word0" : firstWord } )
	
	#####################################################################
	####    Second Word
	#####################################################################
	
	#put 2nd + 3rd words into a new collection
	for j in x:
		grams7.insert( { "word1" : j["word1"] , "word2" : j["word2"] } )
	
	#run map reduce to determine their count within that particular collection
	
	map = Code("function map() { emit( this.word1 , { count : 1 } ); }")
	reduce = Code("function reduce( key , values ) { var result = { count : 0 }; values.forEach( function( value ) { result.count += value.count; }); return result; }")
	
	grams7.map_reduce( map , reduce , "grams8" )
	
	#assign weights to grams
	k = 0
	for j in grams8.find():
		try:
			grams8.update( { "_id" : j["_id"] } , { "lowerBound" : k , "upperBound" : k + j["value"]["count"] } )
		except:
			break
		k = k + j["value"]["count"]

	#pick a random integer
	num = random.randint( 0 , grams8.find().count() - 1 )
	
	secondWord = ""
	
	for j in grams8.find():
		if num >= j["lowerBound"] and num < j["upperBound"]:
			secondWord = j["_id"]
	
	#####################################################################
	####    Third Word
	#####################################################################
	
	y = grams3.find( { "word0" : firstWord , "word1" : secondWord } )
	
	for j in y:
		grams9.insert( { "word2" : j["word2"] } )
	
	map = Code("function map() { emit( this.word2 , { count : 1 } ); }")
	reduce = Code("function reduce( key , values ) { var result = { count : 0 }; values.forEach( function( value ) { result.count += value.count; }); return result; }")
	
	grams9.map_reduce( map , reduce , "grams10" )
	
	#assign weights to grams
	k = 0
	for j in grams10.find():
		grams10.update( { "_id" : j["_id"] } , { "lowerBound" : k , "upperBound" : k + j["value"]["count"] } )
		k = k + j["value"]["count"]
	
	#pick a random integer
	num = random.randint( 0 , grams8.find().count() - 1 )
	
	thirdWord = ""
	
	for j in grams10.find():
		if num >= j["lowerBound"] and num < j["upperBound"]:
			thirdWord = j["_id"]
	
	text = text + firstWord + " " + secondWord + " "
	
	k = thirdWord
	
print text
