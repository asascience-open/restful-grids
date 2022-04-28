mapboxgl.accessToken = 'pk.eyJ1IjoibWF0dC1pYW5udWNjaS1ycHMiLCJhIjoiY2wyaHh3cnZsMGk3YzNlcWg3bnFhcG1yZSJ9.L47O4NS5aFlWgCX0uUvgjA';

const map = new mapboxgl.Map({
    container: document.getElementById('map'),
    style: 'mapbox://styles/mapbox/dark-v8',
    center: [-71, 41],
    zoom: 8,
});

map.on('load', () => {
    map.addSource('ww3', {
        type: 'image', 
        url: '/datasets/ww3/image/?bbox=&parameter=hs&width=256&height=256&cmap=jet&crs=EPSG:4326&datetime=2022-04-12T21:00'

    });

    map.addLayer({
        id: 'ww3', 
        source: 'ww3', 
        type: 'raster', 
        paint: {
            'raster-opacity': 0.8,
        },
    });
});