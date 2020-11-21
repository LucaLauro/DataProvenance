from prov_acquisition.prov_libraries import logic_tracker
import pandas as pd
import numpy as np

global_tracker=0    # instance of a global variable used for tracking with __setitem__


def nan_equal(col_a, col_b):
    try:
        np.testing.assert_equal(col_a, col_b)
    except AssertionError:
        return False
    return True

class ProvenanceTracker:
    def __init__(self, initial_df, provenance_obj):
        self._df = New_df(initial_df)
        #self._callbacks = []
        self._copy_df=New_df(initial_df.copy())
        self.col_add = []
        self.provenance_obj = provenance_obj
        self.shape_change = False
        self.value_change = False
        global global_tracker
        global_tracker=self


    def dataframe_is_changed(self):
        global global_tracker
        if self._df.shape==self._copy_df.shape:
            if not self._df.equals(self._copy_df):
                if len(logic_tracker.columns_list_difference(self._df.columns, self._copy_df.columns)) > 0:
                    # column name change
                    new_col = logic_tracker.columns_list_difference(self._df.columns, self._copy_df.columns)
                    old_col = logic_tracker.columns_list_difference(self._copy_df.columns, self._df.columns)
                    if nan_equal(self._df[new_col].values,self._copy_df[old_col].values):
                        print(f'Column {old_col} renamed in {new_col}')
                else:
                    #print('difference founded')
                    self.value_change = True
                global_tracker = self
            else:
                print('same df')
                pass
        else:
            #print('shape changed detected')
            self.shape_change = True
            global_tracker = self

    def track_provenance(self):
        global global_tracker
        if self.shape_change:
            logic_tracker.shape_change_provenance_tracking(self)
            self._copy_df=self._df.copy()
            self.shape_change = False
            global_tracker = self
        elif self.value_change:
            logic_tracker.value_change_provenance_tracking(self)
            self._copy_df=self._df.copy()
            self.value_change = False
            global_tracker = self

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, new_value):
        #print('set method launched')
        global  global_tracker
        self._df = New_df(new_value)
        self.dataframe_is_changed()
        #self._notify_observers(self._copy_df, self._df)
        if self.shape_change:
            logic_tracker.shape_change_provenance_tracking(self)
            self.shape_change = False
            global_tracker = self
        elif self.value_change:
            logic_tracker.value_change_provenance_tracking(self)
            self.value_change = False
            global_tracker = self
        self._copy_df = self._df.copy()

    def stop_space_prov(self,col_joined):
        print(f'Space transform, {col_joined} generated the new columns {self.col_add}')
        self.col_add = []
        self.provenance_obj.get_prov_space_transformation(self._df, col_joined)
        return


    """def _notify_observers(self, old_value, new_value):
        for callback in self._callbacks:
            callback(old_value, new_value)

    def register_callback(self, callback):
        self._callbacks.append(callback)

    def register_initial_callback(self):
        self.register_callback(self.dataframe_is_changed)"""


class New_df(pd.DataFrame):
    # Override of __setitem__ of pandas, when item is set the changes are recorded
    def __setitem__(self, key, value):
        #print('Override lauched')
        pd.core.frame.DataFrame.__setitem__(self,key, value)
        #obj=inspect.stack()[1][0].f_globals['holder']
        global_tracker.dataframe_is_changed()
        global_tracker.track_provenance()