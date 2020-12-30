/* eslint-disable */
import mx from './graph';
import initGrid from './grid';
import initStyle from './style';
import initTouch from './touch';
import initPorts from './ports';
export { default as initShapes } from './shape';
export { default as initOrgChart } from './orgchart';
export { default as initGrag } from './drag';

const { mxGraph, mxRubberband, mxEventObject, mxEvent } = mx;

export function initGraph (node: HTMLDivElement, dblClick: Function) {

  const graph = new mxGraph(node);

  const dbClick = graph.dblClick;
  graph.dblClick = function(evt: any, cell: any){
    var mxe = new mxEventObject(mxEvent.DOUBLE_CLICK, 'event', evt, 'cell', cell);
    this.fireEvent(mxe);
    if (this.isEnabled() && !mxEvent.isConsumed(evt) && !mxe.isConsumed()) {
      if (cell.isVertex()) {
        dblClick(cell)
      } else {
        dbClick.apply(this, [evt, cell]);
      }
      mxe.consume();
    }
  }

  graph.gridSize = 50;
  new mxRubberband(graph);
  initStyle(graph);
  initGrid(graph);
  initTouch(node, graph);
  initPorts(graph);
  return graph;
}
