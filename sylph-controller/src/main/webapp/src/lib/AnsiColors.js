
import React from 'react';

export class AnsiColors extends React.Component {
    render() {
        let { log } = this.props;
        let regexp = /\\[\d*m/g;
        let symbolList = log.match(regexp);
        let splitList = log.split(regexp);
        let colorMap = {
            '[30m': "black",
            '[31m': "red",
            '[32m': "green",
            '[33m': "#dab633",   //"yellow",
            '[34m': "blue",
            '[35m': "magenta",
            '[36m': "cyan",
            '[37m': "white",
            '[39m': "default",
            '[m': "black"
        }
        let renderList = [];
        let row = []
        for (let i = 0; i < splitList.length; i++) {
            if (i === 0) {
                row.push(<span style={{ color: 'black' }}>{splitList[i]}</span>)
                continue;
            }
            row.push(<span style={{ color: colorMap[symbolList[i - 1]] }}>{splitList[i]}</span>)
        }
        renderList.push(<pre style={{ marginBottom: '0em', overflow: 'visible' }}>{row}</pre>);

        return (
            <div >
                {renderList}
            </div>
        );
    }
}