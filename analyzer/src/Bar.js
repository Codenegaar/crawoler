const cliProgress = require("cli-progress");

module.exports = class Bar {
    constructor() {
        //Create progress bars
        this.multibar = new cliProgress.MultiBar({
            format: this.formatter.bind(this),
            clearOnComplete: false,
            hideCursor: true
        }, cliProgress.Presets.shades_classic);

        let meanSizeTotal = process.env.COMPRESS === 'TRUE' ? 50 : 500;

        this.copressionBar     = this.multibar.create(20, 0, {type: 'compression', compression: process.env.COMPRESS})
        this.totalDiscBar      = this.multibar.create(1000000, 0, {type: 'totalDisc'});
        this.totalFetchedBar   = this.multibar.create(300, 0, {type: 'totalFetched'});
        this.meanOutBar        = this.multibar.create(250, 0, {type: 'meanOut'});
        this.meanSizeBar       = this.multibar.create(meanSizeTotal, 0, {type: 'meanSize'});
        this.meanUrlSizeBar    = this.multibar.create(600, 0, {type:'meanUrlSize'});
    }

    setTotalDiscovered(count) {
        this.totalDiscBar.update(count);
    }
    setTotalFetched(count) {
        this.totalFetchedBar.update(count);
    }
    setMeanOut(value) {
        this.meanOutBar.update(value);
    }
    setMeanSize(value) {
        this.meanSizeBar.update(value);
    }
    setMeanUrlSize(value) {
        this.meanUrlSizeBar.update(value);
    }

    formatter(options, params, payload) {
        let barStr; 
        let percentage = Math.floor(params.progress*100);

        switch (payload.type) {
            case 'compression':
                return `Compression:                ${payload.compression}`;

            case 'totalDisc':
                return `Total links discovered:     ${params.value}`;

            case 'totalFetched':
                return `Total pages fetched:        ${params.value}`;

            case 'meanOut':
                barStr = this.createBarStr(percentage);
                return `Mean Out Degree:            ${barStr} | ${params.value}`;

            case 'meanSize':
                barStr = this.createBarStr(percentage);
                return `Mean Page Size:             ${barStr} | ${params.value} KiB`;

            case 'meanUrlSize':
                barStr = this.createBarStr(percentage);
                return `Mean URL Size:              ${barStr} | ${params.value} Bytes`;

            default:
                return "!!!";
        }
    }
    createBarStr(percentage) {
        let barStr = "";
        for (let i = 0; i < percentage / 2; i++) barStr += "█";
        for (let i = 0; i < 50 - percentage / 2; i++) barStr += "░";
        return barStr;
    }
};