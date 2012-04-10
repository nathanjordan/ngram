import pymongo

################################################################
###############    Function Definitions    #####################
################################################################

#creates a list of tuples with a string and its count
def createGramCounts( source , filename ):
	dest = []
	while len(source):
		s = source[0]
		c = source.count( s )
		removeAllFromList( s , source )
		dest.append( { "gram" : s , "count" : c , "filename" : filename } )
	return dest
		
#removes all of item from a list
def removeAllFromList( item , l ):
	for i in range( 0 , len(l)):
		try:
			j = l.index( item )
			l.pop(j)
		except:
			break

#creates a list of ngrams from a string
def createGramsForN( words , n ):
	grams = []
	for i in range( 0 , len(words) - n + 1 ):
		s = ""
		for j in range( 0 , n ):
			if j == 0:
				s = s + words[i + j]
			else:
				s = s + " " + words[i + j]
		grams.append( s )
	return grams

def createNGrams( fileList , n ):
	gramList = []
	for i in fileList:
		f = open( i , "r" )
		words = f.read().split(" ")
		grams = createGramsForN( words , n )
		gramList.extend( createGramCounts( grams , i ) )
	return gramList
	
################################################################
######################    Program    ###########################
################################################################

##### NGram stuff ##############################################

fileList = [ "seuss.txt" , "seuss2.txt" , "seuss3.txt" ]

nGramDictionary = createNGrams( fileList , 3 )

print nGramDictionary

##### Mongo stuff ##############################################

from pymongo import Connection

connection = Connection()

grams = connection.test.grams

grams.drop()

for i in nGramDictionary:
	grams.insert( i )

