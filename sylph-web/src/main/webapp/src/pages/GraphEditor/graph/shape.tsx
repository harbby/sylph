/* eslint-disable */
import mx from './graph';
import initDrag from './drag';

const {
  mxCell,
  mxGeometry
} = mx;

const createShape = function(sideNode: HTMLDivElement, graph: any, style: string, width: number, height: number) {

  const cell = new mxCell('', new mxGeometry(0, 0, width, height), style);
  cell.vertex = true;
  graph.addCells([cell]);
  const node = graph.view.getCanvas().ownerSVGElement.cloneNode(true);
  node.style.width = width + 'px';
	node.style.height = height + 'px';
  graph.getModel().clear();
  sideNode.appendChild(node)
  initDrag(node, graph, cell);
}

const initShapes =  function(sideNode: HTMLDivElement, graph: any) {
  createShape(sideNode, graph, 'rounded=0;whiteSpace=wrap;html=1', 100, 100);
  createShape(sideNode, graph, 'rounded=1;whiteSpace=wrap;html=1', 100, 100);
  createShape(sideNode, graph, 'shape=rhombus;whiteSpace=wrap;html=1;', 100, 100);
  createShape(sideNode, graph, 'shape=cloud;perimeter=parallelogramPerimeter;whiteSpace=wrap;html=1;fixedSize=1;', 150, 100);
  createShape(sideNode, graph, 'shape=triangle;whiteSpace=wrap;html=1;', 50, 100);
}

export default initShapes
