mapboxgl.accessToken = 'pk.eyJ1IjoibWF0dC1pYW5udWNjaS1ycHMiLCJhIjoiY2wyaHh3cnZsMGk3YzNlcWg3bnFhcG1yZSJ9.L47O4NS5aFlWgCX0uUvgjA';

const map = new mapboxgl.Map({
    container: document.getElementById('map'),
    style: 'mapbox://styles/mapbox/dark-v8',
    center: [-71, 40],
    zoom: 6,
});

map.on('load', () => {
    map.addSource('ww3', {
        type: 'raster',
        tileSize: 512, 
        tiles: [
            '/datasets/ww3/tile/hs/2022-04-12T21:00:00.00/{z}/{x}/{y}?size=512'
        ]
    });

    map.addLayer({
        id: 'ww3', 
        source: 'ww3', 
        type: 'raster', 
        paint: {
            'raster-opacity': 0.5,
        },
    });
});