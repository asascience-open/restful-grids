import * as zarr from 'https://cdn.skypack.dev/@manzt/zarr-lite';

mapboxgl.accessToken = 'pk.eyJ1IjoibWF0dC1pYW5udWNjaS1ycHMiLCJhIjoiY2wyaHh3cnZsMGk3YzNlcWg3bnFhcG1yZSJ9.L47O4NS5aFlWgCX0uUvgjA';

// console.log(zarr);
// const store = new zarr.HTTPStore('http://localhost:9005/datasets/ww3/tree');
// console.log(store);
// const array = await zarr.openArray({store, path: '/0/hs'});
// console.log(array);

// const chunk = await array.getRawChunk([0, 0, 0, 0]);
// console.log(chunk);

class ZarrTileSource {

    constructor({rootUrl, variable, initialTimestep, tileSize = 256}) {
        this.type = 'custom';
        this.tileSize = tileSize;
        this.minzoom = 0; 
        this.maxzoom = 0;

        this.rootUrl = rootUrl;
        this.variable = variable;
        this._timeIndex = initialTimestep;
    }

    get timeIndex() {
        return this._timeIndex;
    }

    set timeIndex(newIndex) {
        this._timeIndex;
    }

    getLevelKey(level) {
        return `/${level}/${this.variable}`;
    }

    async getZarrArray(level) {
        let levelKey = this.getLevelKey(level);

        // TODO: Implement array access and mutex sync 
        return await zarr.openArray({store: this.store, path: levelKey});
    }

    onAdd(map) {
        this.store = new zarr.HTTPStore(this.rootUrl);
    }

    async loadTile({x, y, z}) {
        const array = await this.getZarrArray(z);
        const chunkKey = [0, 0, x, y];

        const rawChunkData = await array.getRawChunk(chunkKey);
        const width = rawChunkData.shape[rawChunkData.shape.length-2];
        const height = rawChunkData.shape[rawChunkData.shape.length-1];
        const tileSizeBytes = width * height;
        const tileSliceStart = this._timeIndex * tileSizeBytes;
        const tileSliceEnd = (this._timeIndex + 1) * tileSizeBytes;
        const rawTileData = rawChunkData.data.slice(tileSliceStart, tileSliceEnd);

        const colorData = new Uint8ClampedArray(4 * width * height); 
        for (let i = 0; i < rawTileData.length; i++) {
            const value = rawTileData[i];
            const r = (value / 5.0) * 255;
            colorData[4 * i] = r;
            colorData[4 * i + 1] = 0;
            colorData[4 * i + 2] = 0;
            colorData[4 * i + 3] = 255;
        }

        return new ImageData(colorData, width);
    };
}

const map = new mapboxgl.Map({
    container: document.getElementById('map'),
    style: 'mapbox://styles/mapbox/dark-v8',
    center: [-71, 40],
    zoom: 0,
});

map.on('load', () => {

    // map.addSource('ww3', {
    //     type: 'raster',
    //     tileSize: 512, 
    //     tiles: [
    //         '/datasets/ww3/tile/hs/2022-04-12T21:00:00.00/{z}/{x}/{y}?size=512'
    //     ]
    // });

    map.addSource('ww3-zarr', new ZarrTileSource({
        rootUrl: 'http://localhost:9005/datasets/ww3/tree',
        variable: 'hs', 
        initialTimestep: 0, 
        tileSize: 256, 
    }));

    map.addLayer({
        id: 'ww3', 
        source: 'ww3-zarr', 
        type: 'raster', 
        paint: {
            'raster-opacity': 0.5,
        },
    });
});