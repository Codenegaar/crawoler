module.exports = class Stats {
    constructor(bar) {
        this.totalFetched = 0;
        this.totalDiscovered = 0;
        this.totalSize = 0;
        this.totalUrlSize = 0;

        this.bar = bar;
    }

    linkDiscovered(url) {
        //Increase totalDiscovered
        this.totalDiscovered++;

        //Increase totalUrlSize
        this.totalUrlSize += url.length * 2;

        //Update bar
        this.bar.setTotalDiscovered(this.totalDiscovered);
        this.bar.setMeanUrlSize(Math.floor(
            this.totalUrlSize / this.totalDiscovered
        ));
        this.bar.setMeanOut(Math.floor(
            this.totalDiscovered / this.totalFetched
        ));
    }

    pageFetched(size) {
        //Increase totalFetched
        this.totalFetched++;
        //Increase totalSize
        this.totalSize += size;

        //Update bar
        this.bar.setTotalFetched(this.totalFetched);
        this.bar.setMeanSize(Math.floor(
            (this.totalSize / this.totalFetched) / 1024
        ));
    }
};