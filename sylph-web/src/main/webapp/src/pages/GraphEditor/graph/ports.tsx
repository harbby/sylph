/* eslint-disable */
import mx from './graph';

const { mxShape, mxEdgeHandler, mxConnectionConstraint, mxPoint, mxConstants, mxTriangle } = mx;

export default function initPorts(graph: any) {

  graph.setConnectable(true);
  graph.setAllowDanglingEdges(false);
  graph.setMultigraph(false);
  // Disables automatic handling of ports. This disables the reset of the
  // respective style in mxGraph.cellConnected. Note that this feature may
  // be useful if floating and fixed connections are combined.
  graph.setPortsEnabled(false);

  // Enables rubberband selection
  // new mxRubberband(graph);

  // Gets the default parent for inserting new cells. This
  // is normally the first child of the root (ie. layer 0).
  // var parent = graph.getDefaultParent();

  // Ports are equal for all shapes...
  var ports = new Array();

  // NOTE: Constraint is used later for orthogonal edge routing (currently ignored)
  ports['w'] = { x: 0, y: 0.5, perimeter: true, constraint: 'west' };
  ports['e'] = { x: 1, y: 0.5, perimeter: true, constraint: 'east' };
  ports['n'] = { x: 0.5, y: 0, perimeter: true, constraint: 'north' };
  ports['s'] = { x: 0.5, y: 1, perimeter: true, constraint: 'south' };
  ports['nw'] = { x: 0, y: 0, perimeter: true, constraint: 'north west' };
  ports['ne'] = { x: 1, y: 0, perimeter: true, constraint: 'north east' };
  ports['sw'] = { x: 0, y: 1, perimeter: true, constraint: 'south west' };
  ports['se'] = { x: 1, y: 1, perimeter: true, constraint: 'south east' };

  // ... except for triangles
  var ports2 = new Array();

  // NOTE: Constraint is used later for orthogonal edge routing (currently ignored)
  ports2['in1'] = { x: 0, y: 0, perimeter: true, constraint: 'west' };
  ports2['in2'] = { x: 0, y: 0.25, perimeter: true, constraint: 'west' };
  ports2['in3'] = { x: 0, y: 0.5, perimeter: true, constraint: 'west' };
  ports2['in4'] = { x: 0, y: 0.75, perimeter: true, constraint: 'west' };
  ports2['in5'] = { x: 0, y: 1, perimeter: true, constraint: 'west' };

  ports2['out1'] = { x: 0.5, y: 0, perimeter: true, constraint: 'north east' };
  ports2['out2'] = { x: 1, y: 0.5, perimeter: true, constraint: 'east' };
  ports2['out3'] = { x: 0.5, y: 1, perimeter: true, constraint: 'south east' };

  // Extends shapes classes to return their ports
  mxShape.prototype.getPorts = function () {
    return ports;
  };

  mxTriangle.prototype.getPorts = function () {
    return ports2;
  };

  // Disables floating connections (only connections via ports allowed)
  graph.connectionHandler.isConnectableCell = function (cell: any) {
    return false;
  };
  mxEdgeHandler.prototype.isConnectableCell = function (cell: any) {
    return graph.connectionHandler.isConnectableCell(cell);
  };

  // Disables existing port functionality
  graph.view.getTerminalPort = function (state: any, terminal: any, source: any): any {
    return terminal;
  };

  // Returns all possible ports for a given terminal
  graph.getAllConnectionConstraints = function (terminal: any, source: any) {
    if (terminal != null && terminal.shape != null &&
      terminal.shape.stencil != null) {
      // for stencils with existing constraints...
      if (terminal.shape.stencil != null) {
        return terminal.shape.stencil.constraints;
      }
    }
    else if (terminal != null && this.model.isVertex(terminal.cell)) {
      if (terminal.shape != null) {
        var ports = terminal.shape.getPorts();
        var cstrs = new Array();

        for (var id in ports) {
          var port = ports[id];

          var cstr = new mxConnectionConstraint(new mxPoint(port.x, port.y), port.perimeter);
          cstr.id = id;
          cstrs.push(cstr);
        }

        return cstrs;
      }
    }

    return null;
  };

  // Sets the port for the given connection
  graph.setConnectionConstraint = function (edge: any, terminal: any, source: any, constraint: any) {
    if (constraint != null) {
      var key = (source) ? mxConstants.STYLE_SOURCE_PORT : mxConstants.STYLE_TARGET_PORT;

      if (constraint == null || constraint.id == null) {
        this.setCellStyles(key, null, [edge]);
      }
      else if (constraint.id != null) {
        this.setCellStyles(key, constraint.id, [edge]);
      }
    }
  };

  // Returns the port for the given connection
  graph.getConnectionConstraint = function (edge: any, terminal: any, source: any) {
    var key = (source) ? mxConstants.STYLE_SOURCE_PORT : mxConstants.STYLE_TARGET_PORT;
    var id = edge.style[key];

    if (id != null) {
      var c = new mxConnectionConstraint(null, null);
      c.id = id;
      return c;
    }
    return null;
  };

  // Returns the actual point for a port by redirecting the constraint to the port
  let graphGetConnectionPoint = graph.getConnectionPoint;
  graph.getConnectionPoint = function (vertex: any, constraint: any) {
    if (constraint.id != null && vertex != null && vertex.shape != null) {
      var port = vertex.shape.getPorts()[constraint.id];

      if (port != null) {
        constraint = new mxConnectionConstraint(new mxPoint(port.x, port.y), port.perimeter);
      }
    }

    return graphGetConnectionPoint.apply(this, [vertex, constraint]);
  };

}
