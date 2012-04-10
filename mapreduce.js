function map() {
	
	emit( this.gram , { "count" : this.count } )
	
	}
	
function reduce( key , values ) {
	
	var result = { "count" : 0 }
	
	values.forEach( function( value ) {
		
		result.count += value.count;
		
		});
	
	return result;
	
	}
