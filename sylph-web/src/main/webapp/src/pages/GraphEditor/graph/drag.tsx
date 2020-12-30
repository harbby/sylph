/* eslint-disable */
import mx from './graph';

const {
  mxDragSource,
  mxUtils,
  mxEvent,
} = mx;

export default function initDrag(dragNode: HTMLDivElement, graph: any, cell: any) {

  var graphF = function(evt: any)
  {
    var x = mxEvent.getClientX(evt);
    var y = mxEvent.getClientY(evt);
    var elt = document.elementFromPoint(x, y);
    
    if (mxUtils.isAncestorNode(graph.container, elt))
    {
      return graph;
    }
    
    return null;
  };
  
  // Inserts a cell at the given location
  var funct = function(graph: any, evt: any, target: any, x: any, y: any)
  {
    var cells = graph.importCells([cell], x, y, target);

    if (cells != null && cells.length > 0)
    {
      graph.scrollCellToVisible(cells[0]);
      graph.setSelectionCells(cells);
    }
  };
  var ds = mxUtils.makeDraggable(dragNode, graphF, funct);
  ds.isGuidesEnabled = function() {
    return graph.graphHandler.guidesEnabled;
  };
  ds.createDragElement = mxDragSource.prototype.createDragElement;
}