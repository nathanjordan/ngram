import pymongo
import sys

################################################################
###############    Function Definitions    #####################
################################################################

#creates a list of tuples with a string and its count
def countGrams( source , filename ):
	dest = []
	while len(source):
		s = source[0]["gram"]
		words = source[0]["words"]
		c = 0
		for i in range( 0 , len( source ) ):
			if source[i]["gram"] == s:
				c += 1
		removeAllFromList( s , source )
		dest.append( { "gram" : s , "words" : words , "count" : c , "filename" : filename } )
	return dest
		
#removes all of item from a list
def removeAllFromList( item , l ):
	found = True
	while found:
		found = False
		for i in range( 0 , len(l) ):
			if l[i]["gram"] == item:
				l.pop( i )
				found = True
				break
	

#creates a list of ngrams from a string
def createGramsForN( words , n ):
	grams = []
	for i in range( 0 , len(words) - n + 1 ):
		s = ""
		individualWords = []
		for j in range( 0 , n ):
			individualWords.append( words[i + j] )
			if j == 0:
				s = s + words[i + j]
			else:
				s = s + " " + words[i + j]
		grams.append( { "gram" : s , "words" : individualWords } )
	return grams

def createNGramList( fileList , n ):
	gramList = []
	for i in fileList:
		f = open( i , "r" )
		words = f.read().split(" ")
		grams = createGramsForN( words , n )
		gramList.extend( countGrams( grams , i ) )
		f.close()
	return gramList
	
################################################################
######################    Program    ###########################
################################################################

##### NGram stuff ##############################################

fileList = [ "speare.txt" ]

nGramDictionary = createNGramList( fileList , 3 )

##### Mongo stuff ##############################################

from pymongo import Connection
from pymongo.code import Code

connection = Connection()
grams = connection.test.grams
grams2 = connection.test.grams2
grams3 = connection.test.grams3
grams4 = connection.test.grams4
grams5 = connection.test.grams5
grams6 = connection.test.grams6

# Reset grams database
grams.drop()
grams2.drop()
grams3.drop()
grams4.drop()
grams5.drop()
grams6.drop()

# insert all the new grams for testing purposes
for i in nGramDictionary:
	grams.insert( i )

map = Code("function map() { emit( this.gram , { count : this.count , words : this.words } ); }")
reduce = Code("function reduce( key , values ) { var result = { count : 0 , words : values[0].words }; values.forEach( function( value ) { result.count += value.count; }); return result; }")

#map reduce that shizzz
grams.map_reduce( map , reduce , "grams2" )

for i in grams2.find():
	grams3.insert( { "gram" : i["_id"] , "word0" : i["value"]["words"][0] , "word1" : i["value"]["words"][1] , "word2" : i["value"]["words"][2] , "count" : i["value"]["count"] } )
	
words = []
#create individual words
for i in fileList:
	f = open( i , "r" )
	words.extend( f.read().split(" ") )
	
for i in words:
	grams4.insert( { "word" : i } )
	
map = Code("function map() { emit( this.word , { count : 1 } ); }")
reduce = Code("function reduce( key , values ) { var result = { count : 0 }; values.forEach( function( value ) { result.count += value.count; }); return result; }")

#map reduce that shizzz
grams4.map_reduce( map , reduce , "grams5" )

j = 0
for i in grams5.find():
	grams6.insert( { "word" : i["_id"] , "count" : i["value"]["count"] , "lowerBound" : j , "upperBound" : j + i["value"]["count"] } )
	j += i["value"]["count"]


	
	
	
	
	
	
	
	
