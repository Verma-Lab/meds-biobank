from meds_biobank.plotters.plotters import plot_events

def test_plotters(spark, meds_events):

    # convert meds_events to dict
    events_list = [row.asDict() for row in meds_events.collect()]

    # plot
    plot_events(events_list)