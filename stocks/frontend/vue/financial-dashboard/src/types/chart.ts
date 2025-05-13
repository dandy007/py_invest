export interface ChartLayout {
  showlegend: boolean;
  xaxis: {
    title: string;
    gridcolor: string;
    type?: 'date';
    range?: number[];
    fixedrange?: boolean;
  };
  yaxis: {
    title: string;
    gridcolor: string;
    tickformat?: string;
    ticksuffix?: string;
    fixedrange?: boolean;
  };
  height?: number;
  margin?: {
    t: number;
    b: number;
  };
}

export interface PlotlyTrace {
  x: (string | number)[];
  y: number[];
  type: 'scatter' | 'bar' | 'histogram';
  mode?: 'lines' | 'markers' | 'lines+markers';
  name?: string;
  line?: {
    color?: string;
    width?: number;
  };
  marker?: {
    color?: string;
    line?: {
      color?: string;
      width?: number;
    };
    size?: number;
  };
  histnorm?: 'percent' | 'probability' | 'density' | 'probability density';
  nbinsx?: number;
  hovertemplate?: string;
  customdata?: any[][];
}
