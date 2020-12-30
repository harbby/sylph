/* eslint-disable */
import mx from './graph';
import { Base64 } from 'js-base64';

const { mxConstants, mxImage, mxOutline, mxVertexHandler, mxEdgeHandler, mxEvent } = mx;

const createSvgImage = function(w: number, h: number, data: string, coordWidth?: number, coordHeight?: number)
{
	var tmp = unescape(encodeURIComponent(
        '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">' +
        '<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="' + w + 'px" height="' + h + 'px" ' +
        ((coordWidth != null && coordHeight != null) ? 'viewBox="0 0 ' + coordWidth + ' ' + coordHeight + '" ' : '') +
        'version="1.1">' + data + '</svg>'));

    return new mxImage('data:image/svg+xml;base64,' + ((window.btoa) ? btoa(tmp) : Base64.encode(tmp, true)), w, h)
};

const mainHandle= createSvgImage(18, 18, '<circle cx="9" cy="9" r="5" stroke="#fff" fill="#29b6f2" stroke-width="1"/>');
const rotationHandle = createSvgImage(16, 16, '<path stroke="#29b6f2" fill="#29b6f2" d="M15.55 5.55L11 1v3.07C7.06 4.56 4 7.92 4 12s3.05 7.44 7 7.93v-2.02c-2.84-.48-5-2.94-5-5.91s2.16-5.43 5-5.91V10l4.55-4.45zM19.93 11c-.17-1.39-.72-2.73-1.62-3.89l-1.42 1.42c.54.75.88 1.6 1.02 2.47h2.02zM13 17.9v2.02c1.39-.17 2.74-.71 3.9-1.61l-1.44-1.44c-.75.54-1.59.89-2.46 1.03zm3.89-2.42l1.42 1.41c.9-1.16 1.45-2.5 1.62-3.89h-2.02c-.14.87-.48 1.72-1.02 2.48z"/>', 24, 24);

export default function initStyle(graph: any) {
  mxConstants.HANDLE_FILLCOLOR = '#99ccff';
  mxConstants.HANDLE_STROKECOLOR = '#0088cf';
  mxConstants.VERTEX_SELECTION_COLOR = '#00a8ff';
  mxConstants.SHADOWCOLOR = '#000000';
  mxConstants.VML_SHADOWCOLOR = '#d0d0d0';
  mxConstants.SHADOW_OPACITY = 0.25;
  mxConstants.DEFAULT_VALID_COLOR = "#00a8ff";
  mxConstants.HIGHLIGHT_SIZE = 5;
  mxConstants.HIGHLIGHT_OPACITY = 30;
  mxConstants.EDGE_SELECTION_COLOR =  "#00a8ff";
  mxConstants.PERIMETER_TRIANGLE = "trianglePerimeter";
  mxConstants.POINTS = 1;
  mxConstants.MILLIMETERS = 2;
  mxConstants.INCHES = 3;
  mxConstants.PIXELS_PER_MM = 3.937;
  mxConstants.PIXELS_PER_INCH = 100;

  // Sets constants for touch style
  mxConstants.HANDLE_SIZE = 6;
  mxConstants.LABEL_HANDLE_SIZE = 7;
  var style = graph.getStylesheet().getDefaultVertexStyle();
  style['fillColor'] = '#FFFFFF';
  style['strokeColor'] = '#000000';
  style['fontColor'] = '#000000';
  style['fontStyle'] = '1';
  
  style = graph.getStylesheet().getDefaultEdgeStyle();
  style['strokeColor'] = '#000000';
  style['fontColor'] = '#000000';
  style['fontStyle'] = '0';
  style['fontStyle'] = '0';
  style['startSize'] = '8';
  style['endSize'] = '10';

  mxVertexHandler.prototype.handleImage = mainHandle;
  mxEdgeHandler.prototype.handleImage = mainHandle;
  mxOutline.prototype.sizerImage = mainHandle;
  
  var vertexHandlerCreateSizerShape = mxVertexHandler.prototype.createSizerShape;
  mxVertexHandler.prototype.createSizerShape = function(bounds: any, index: any, fillColor: any)
  {
    this.handleImage = (index == mxEvent.ROTATION_HANDLE) ? rotationHandle : (index == mxEvent.LABEL_HANDLE) ? this.secondaryHandleImage : this.handleImage;
    
    return vertexHandlerCreateSizerShape.apply(this, arguments);
  };
}
