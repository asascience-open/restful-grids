import * as zarr from 'https://cdn.skypack.dev/@manzt/zarr-lite';

mapboxgl.accessToken = 'pk.eyJ1IjoibWF0dC1pYW5udWNjaS1ycHMiLCJhIjoiY2wyaHh3cnZsMGk3YzNlcWg3bnFhcG1yZSJ9.L47O4NS5aFlWgCX0uUvgjA';

console.log(zarr);
const store = new zarr.HTTPStore('http://localhost:9005/datasets/ww3/tree');
console.log(store);
const array = await zarr.openArray({store, path: '/0/hs'});
console.log(array);

const chunk = await array.getRawChunk([0, 0, 0, 0]);
console.log(chunk);

const map = new mapboxgl.Map({
    container: document.getElementById('map'),
    style: 'mapbox://styles/mapbox/dark-v8',
    center: [-71, 40],
    zoom: 6,
});

map.on('load', () => {

    // map.addSource('ww3', {
    //     type: 'raster',
    //     tileSize: 512, 
    //     tiles: [
    //         '/datasets/ww3/tile/hs/2022-04-12T21:00:00.00/{z}/{x}/{y}?size=512'
    //     ]
    // });

    // map.addLayer({
    //     id: 'ww3', 
    //     source: 'ww3', 
    //     type: 'raster', 
    //     paint: {
    //         'raster-opacity': 0.5,
    //     },
    // });
});