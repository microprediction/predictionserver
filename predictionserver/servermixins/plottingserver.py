from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.servermixins.laggedserver import LaggedServer
from predictionserver.serverhabits.plottinghabits import PlottingHabits
import json
import datetime
import plotly
import pandas as pd
import plotly.graph_objs as go


class PlottingServer(PlottingHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ------------
        #   Plotting
        # ------------

    def lagged_bar(self, name):
        """
        :param name:
        :return:    graphJSON
        """
        try:
            lagged_values, lagged_times = self.get_lagged_values_and_times(name=name)
            lagged_dt = [datetime.datetime.fromtimestamp(t).strftime('%c') for t in lagged_times]
            df = pd.DataFrame({'t': reversed(lagged_dt), 'v': reversed(lagged_values)})  # creating a sample dataframe
            data = [
                go.Bar(
                    x=df['t'],
                    y=df['v']
                )
            ]
            graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
        except:
            df = pd.DataFrame({'t': [], 'v': []})  # creating a sample dataframe
            data = [
                go.Bar(
                    x=df['t'],
                    y=df['v']
                )
            ]
            graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
        return graphJSON

    def cdf_bar(self, name, delay=None):
        try:
            cdf = self.get_cdf(name=name, delay=int(delay))
            df = pd.DataFrame({'x': cdf['x'], 'y': cdf['y']})  # creating a sample dataframe
            data = [
                go.scatter.Line(
                    x=df['x'],
                    y=df['y']
                )]
            graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
        except:
            # hardcoded dummy values
            df = pd.DataFrame({'x': [], 'y': []})  # creating a sample dataframe
            data = [
                go.Line(
                    x=df['x'],
                    y=df['y']
                )]
            graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
        return graphJSON


class StandalonePlottingServer(PlottingServer,LaggedServer,BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


if __name__=='__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, FLATHAT_STOAT
    server = StandalonePlottingServer(**REDIZ_COLLIDER_CONFIG)

