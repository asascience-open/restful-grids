import * as zarr from 'https://cdn.skypack.dev/@manzt/zarr-lite';

mapboxgl.accessToken = 'pk.eyJ1IjoibWF0dC1pYW5udWNjaS1ycHMiLCJhIjoiY2wyaHh3cnZsMGk3YzNlcWg3bnFhcG1yZSJ9.L47O4NS5aFlWgCX0uUvgjA';

// From https://github.com/notenoughneon/await-semaphore/blob/master/index.ts
export class Semaphore {

    constructor(count) {
        this.count = count;
        this.tasks = [];
    }

    sched() {
        if (this.count > 0 && this.tasks.length > 0) {
            this.count--;
            let next = this.tasks.shift();
            if (next === undefined) {
                throw "Unexpected undefined value in tasks list";
            }

            next();
        }
    }

    acquire() {
        return new Promise((res, _) => {
            var task = () => {
                var released = false;
                res(() => {
                    if (!released) {
                        released = true;
                        this.count++;
                        this.sched();
                    }
                });
            };
            this.tasks.push(task);
           
            setTimeout(this.sched.bind(this), 0);
            //setImmediate(this.sched.bind(this));
        });
    }

    use(f) {
        return this.acquire()
        .then(release => {
            return f()
            .then((res) => {
                release();
                return res;
            })
            .catch((err) => {
                release();
                throw err;
            });
        });
    }
}

export class Mutex extends Semaphore {
    constructor() {
        super(1);
    }
}


class ZarrTileSource {

    constructor({ rootUrl, variable, initialTimestep, tileSize = 256, minZoom = 0, maxZoom = 10, bounds }) {
        this.type = 'custom';
        this.tileSize = tileSize;
        this.minZoom = minZoom; 
        this.maxZoom = maxZoom;
        this.bounds = bounds;

        this.rootUrl = rootUrl + `/${minZoom},${maxZoom}/${tileSize}`;
        this.variable = variable;
        this._timeIndex = initialTimestep;
    }

    /**
     * Get the current time index
     */
    get timeIndex() {
        return this._timeIndex;
    }

    /**
     * Set the time index to the given value.
     * @param {number} timeIndex
     */
    set timeIndex(newIndex) {
        this._timeIndex = newIndex;
        // TODO: For now the reload has to be triggered from user space
    }

    getLevelKey(level) {
        return `/${level}/${this.variable}`;
    }

    async getZarrArray(level) {
        let levelKey = this.getLevelKey(level);

        const array = await this.zarrayMutex.use(async () => {
            let array = this.arrayCache[levelKey];

            if (!array) {
                array = await zarr.openArray({store: this.store, path: levelKey});
                this.arrayCache[levelKey] = array;
            }
            return array;
        });

        return array;
    }

    async onAdd(map) {
        this.store = new zarr.HTTPStore(this.rootUrl);
        this.zarrayMutex = new Mutex();
        this.chunkCache = {};
        this.arrayCache = {};
    }

    async loadTile({ x, y, z }) {
        const array = await this.getZarrArray(z);
        const chunkKey = `0.0.${x}.${y}`;

        let rawChunkData = this.chunkCache[chunkKey];
        if (!rawChunkData) {
            rawChunkData = await array.getRawChunk(chunkKey);
            this.chunkCache[chunkKey] = rawChunkData;
        }

        const width = rawChunkData.shape[rawChunkData.shape.length - 2];
        const height = rawChunkData.shape[rawChunkData.shape.length - 1];
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
            colorData[4 * i + 3] = isNaN(value) ? 0 : 255;
        }

        return new ImageData(colorData, width);
    };
}

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

    map.addSource('ww3-zarr', new ZarrTileSource({
        rootUrl: 'http://localhost:9005/datasets/ww3/tree',
        variable: 'hs',
        initialTimestep: 0,
        tileSize: 256,
        bounds: [-93.0, 20.0, -55.0, 55.0],
    }));

    map.addLayer({
        id: 'ww3',
        source: 'ww3-zarr',
        type: 'raster',
        paint: {
            'raster-opacity': 1.0,
            'raster-fade-duration': 0,
        },
    });

    const zarrSource = map.getSource('ww3-zarr');

    let timestepSlider = document.getElementById('timestep-slider');
    timestepSlider.oninput = e => {
        const newTimeIndex = e.target.valueAsNumber;
        zarrSource._implementation.timeIndex = newTimeIndex;
        zarrSource.load();
    }
    // timestepSlider.onchange = e => {
    //     const newTimeIndex = e.target.valueAsNumber;
    //     zarrSource._implementation.timeIndex = newTimeIndex;
    //     zarrSource.load();
    // };
});