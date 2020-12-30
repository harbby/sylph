/* eslint-disable */
import mx from './graph';

const {
  mxOutline
} = mx;

export default function initOrgChart(outlineNode: HTMLDivElement, graph: any) {
  new mxOutline(graph, outlineNode);
}