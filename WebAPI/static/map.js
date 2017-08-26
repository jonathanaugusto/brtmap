      var map;
      var markers = [];
      var polygon = null;
      var countries = [];
      var result_gps = [];
      var markers = [];
      var has_markers = true;

      function initMap(){

        map = new google.maps.Map(document.getElementById('map'), {
        zoom: 12,
        center: {lat: -22.913767, lng: -43.491655}, 
        mapTypeId: google.maps.MapTypeId.ROADMAP

        });
        
        $('#metricSelect').change(function() {
    	    var linha = $(this).val();
    	    has_markers = true;
    	    clearOverlays();
    	    markers = [];
    	  	var request = $.ajax({
    		  url: "/brtpos",
    		  type: "POST",
    		  data : {linha:linha},
    		  dataType: "json"
    		});
        	request.done(function(result) {
        		result_gps = result;
        		for (var pin in result)
    			{
        			var marker = new google.maps.Marker({
        		          position: result[pin],
        		          map: map,
        		          icon: result[pin].icon,
        		          title: result[pin].title
        		        });
        			markers.push(marker);
        		}
    	    
        	});
        });
        
        heatmap = new HeatmapOverlay(map, 
		{
		// radius should be small ONLY if scaleRadius is true (or small radius is intended)
			"radius": 10,
			"maxOpacity": 1, 
		// scales the radius based on map zoom
			"scaleRadius": false, 
		// if set to false the heatmap uses the global maximum for colorization
		// if activated: uses the data maximum within the current map boundaries 
		//   (there will always be a red spot with useLocalExtremas true)
			"useLocalExtrema": true,
		// which field name in your data represents the latitude - default "lat"
			latField: 'lat',    // which field name in your data represents the longitude - default "lng"
			lngField: 'lng', // which field name in your data represents the data value - default "value"
			valueField: 'count'
		});

    }
    
    function loadStationMarkers() {
    	$.getJSON("/static/estacoes.json", function(json) {
    	    for(var i in json){
    	    	var lat = parseFloat(json[i].latitude);
    	    	var lng = parseFloat(json[i].longitude);
    	    	var marker = new google.maps.Marker({
  		          position: {"lat":lat, "lng":lng},
  		          map: map,
  		          icon: "/static/estacao.png",
  		          title: json[i].estacao
  		        });
    	    }
    	});
    }
      
    function loadMarkers(){
    	var conceptName = $('#metricSelect').find(":selected").text();
    	var request = $.ajax({
    		  url: "/brtpos",
    		  type: "POST",
    		  data : {linha:conceptName},
    		  dataType: "json"
    		});
    	request.done(function(result) {
    		console.log(result);
    		clearOverlays();
    		markers = [];
    		result_gps = result;
    		for (var pin in result)
			{
    			var marker = new google.maps.Marker({
    		          position: result[pin],
    		          map: map,
    		          icon: result[pin].icon,
    		          title: result[pin].title
    		        });
    			markers.push(marker);
    		}
	    
    	});
    }	
    
    function clearOverlays()
    {
    	if(has_markers)
		{
    		for(var point in markers)
    		{
        		markers[point].setMap(null);
    		}
    		
		}
    	else {
    		for(var point in markers)
    		{
        		markers[point].setMap(map);
    		}
    		var testData = {
        		max : 1000,
        		data : []
        	};
    		heatmap.setData(testData);
    	}
    	has_markers = !has_markers;
    }
    
    function getPoints() {
    	var list = [];
    	for(var point in result_gps)
    	{
        	list.push({"lat":result_gps[point].lat, "lng":result_gps[point].lng, "count":result_gps[point].count});
    	}
    	return list;
    }

    function generateHeatmap(){
    	clearOverlays();
    	var linha = $('#metricSelect').find(":selected").text();
    	var request = $.ajax({
  		  url: "/heatMap",
  		  type: "POST",
  		  data : {linha:linha},
  		  dataType: "json"
  		});
      	request.done(function(result) {
      		result_gps = result;
      		if(!has_markers){
        		var points = getPoints();
            	var testData = {
            		max : 100,
            		data : result_gps
            	};
            	
        	    heatmap.setData(testData);
        	}
  	    
      	});
    }

    