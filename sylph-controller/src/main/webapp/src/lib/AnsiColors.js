
import React from 'react';

export class AnsiColors extends React.Component {
    render() {
        let { log } = this.props;
        let regexp = /\\[\d*m/g;
        let symbolList = log.match(regexp);
        let splitList = log.split(regexp);
        //debugger
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
        for (let i = 0; i < splitList.length; i++) {
            if (i === 0) {
                renderList.push({
                    color: "black",
                    val: splitList[i]
                })
                continue;
            }
            renderList.push({
                color: colorMap[symbolList[i - 1]],
                val: splitList[i]
            })
        }


        return (
            <div>
                {(() => {
                    return renderList.map(l => <pre style={{ color: l.color, marginBottom: '0em' }}>{l.val}</pre>)
                })()}
            </div>
        );
    }
}