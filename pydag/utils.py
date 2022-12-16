import logging
import networkx as nx
import matplotlib.pyplot as plt
from rich.logging import RichHandler


def draw_graph(g):
    options = {
        "font_size": 10,
        "node_size": 2000,
        "node_color": "#ff8c00",
        "edgecolors": "black",
        "linewidths": 1,
        "width": 2,
        "with_labels": True,
        "alpha": 0.8,
    }
    plt.figure(1,figsize=(12,12))
    pos = nx.nx_pydot.pydot_layout(g, prog="dot")
    nx.draw(g, pos, **options)
    plt.show()


def prepare_rich_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = RichHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("%(message)s")

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    return logger
