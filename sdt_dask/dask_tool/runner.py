"""
This module provides a class to run the SolarDataTools pipeline on a Dask 
Cluster. It takes a Dask Client as input and use the DataPlug to retrieve 
datasets to run with the pipeline on the Dask Client after setting up the task 
graph.

See the README and tool_demo_SDTDask.ipynb for more information.

"""
import os
import pandas as pd
from dask import delayed
from time import strftime
from dask.distributed import performance_report
from solardatatools import DataHandler

class Runner:
    """A class to run the SolarDataTools pipeline on a Dask cluster.
    Handles invalid data keys and failed datasets.

    :param client: The initialized Dask Client Object to submit the task 
        graph for computations.
    :type client: Dask Client Object
    :param output_path: Directory path to save reports and results DataFrame.
    :type output_path: string
    """

    
    def __init__(self, client, output_path="../results/"):
        self.client = client
        self.output_path = output_path


    def set_up(self, KEYS, data_plug, **kwargs):
        """Function to retrieve datasets using the keys and DataPlugs. Sets up 
        the pipeline on the Dask Client by creating a task graph.

        Calls run_pipeline functions in a for loop over the keys
        and appends results to a DataFrame.

        :param keys: List of tuples to used to access PV datasets.
        :type keys: list
        :param kwargs: Additional arguments for solardatatools run_pipeline.
        :type kwargs: dict
        """

        def get_data(key):
            """Creates a DataFrame for a key that includes the errors for the
            functions performed on the file and its DataHandler.

            :param key: The key combination of the file.
            :type key: tuple
            :return: Returns the dataframe for the key and its DataHandler.
            :rtype: tuple
            """

            # Addition of function error status for key
            errors = {"get_data error": ["No error"],
                      "run_pipeline error": ["No error"],
                      "run_pipeline_report error": ["No error"],
                      "run_loss_analysis error": ["No error"],
                      "run_loss_analysis_report error": ["No error"]}

            # Creates a key dictionary for the key combinations and the sensor
            # information
            key_dict = {}
            for i in range(len(key)):
                key_dict[f"key_field_{i}"] = key[i]

            # Combines the key and errors into a dictionary and generates a
            # dataframe
            data_dict = {**key_dict, **errors}
            data_df = pd.DataFrame.from_dict(data_dict)

            try:
                # Reads CSV file and creates dataframe, Here the function reads
                # data into pandas and python instead of using Dask to store the
                # data in memory for further computation **.
                df = data_plug.get_data(key)
                dh = DataHandler(df)
                return (data_df, dh)
            except Exception as e:
                data_df["get_data error"] = str(e)
                return (data_df,)
            

        def run_pipeline(data_tuple, **kwargs):
            """Function runs the pipeline and appends the results to the
            dataframe. The function also stores the exceptions for the function
            call into its respective errors.

            :param data_tuple: The tuple consists of the dataframe and 
                DataHandler.
            :type data_tuple: tuple
            :param kwargs: The keyword arguments passed to the DataHandler's
                run_pipeline.
            :type kwargs: dict
            :return: Tuple containing key's pipeline results DataFrame and 
                DataHandler.
            :rtype: tuple
            """

            # Assigns the dataframe, the first element of the tuple.
            data_df = data_tuple[0]

            # Change the errors if no DataHandler is created
            if data_df.iloc[0]["get_data error"] != "No error":
                error = "get_data error lead to nothing"
                data_df["run_pipeline error"] = error
                data_df["run_pipeline_report error"] = error
                data_df["run_loss_analysis error"] = error
                data_df["run_loss_analysis_report error"] = error
                return (data_df,)

            # Calls DataHandler's run_pipeline and handles errors
            else:
                datahandler = data_tuple[1]

                try:
                    datahandler.run_pipeline(**kwargs)
                    if datahandler.num_days <= 365:
                        data_df[
                            "run_loss_analysis error"] = "The length of data is less than or equal to 1 year, loss analysis will fail thus is not performed."
                        data_df[
                            "run_loss_analysis_report error"] = "Loss analysis is not performed"

                except Exception as e:
                    data_df["run_pipeline error"] = str(e)
                    error = "Failed because of run_pipeline error"
                    data_df["run_loss_analysis error"] = error
                    data_df["run_pipeline_report error"] = error
                    data_df["run_loss_analysis_report error"] = error


            # Gets the run_pipeline report and appends it to the dataframe as
            # columns and handles errors
            if data_df.iloc[0]["run_pipeline error"] == "No error":
                try:
                    report = datahandler.report(return_values=True,
                                                verbose=False)
                    data_df = data_df.assign(**report)
                except Exception as e:
                    data_df["run_pipeline_report error"] = str(e)
                    print(e)
                # Gets the runtime for run_pipeline
                try:
                    data_df["runtimes"] = datahandler.total_time
                except Exception as e:
                    print(e)

            return (data_df, datahandler)
        

        def run_loss_analysis(data_tuple):
            """Runs the Loss analysis on the pipeline, handles errors and saves
            the loss report results by appending it to the key dataframe. All
            errors are assigned to the key dataframe in error reports.

            :param data_tuple: A tuple containing the key dataframe and the
                datahandler object.
            :type data_tuple: tuple
            :return: key dataframe with appended reports and assigned error 
                values.
            :rtype: Pandas DataFrame
            """
            data_df = data_tuple[0]

            if data_df.iloc[0]["run_loss_analysis error"] == "No error":
                datahandler = data_tuple[1]
                try:
                    datahandler.run_loss_factor_analysis(verbose=True)
                except Exception as e:
                    data_df["run_loss_analysis error"] = str(e)
                    error = "Failed because of run_loss_analysis error"
                    data_df["run_loss_analysis_report error"] = error
                try:
                    loss_report = datahandler.loss_analysis.report()
                    data_df = data_df.assign(**loss_report)
                except Exception as e:
                    data_df["run_loss_analysis_report error"] = str(e)

            return data_df

        results = []
        
        # For larger number of files it is recommended to use Dask collections
        # instead of a for loop **
        # Reference:
        #   https://docs.dask.org/en/latest/delayed-best-practices.html#avoid-too-many-tasks
        for key in KEYS:
            data_tuple_0 = delayed(get_data)(key)
            # data_tuple_0 = delayed(data_tuple_0)
            data_tuple_1 = delayed(run_pipeline)(data_tuple_0, fix_shifts=True,
                                                 verbose=False)
            # data_tuple_1 = delayed(data_tuple_1)
        
            result_df = delayed(run_loss_analysis)(data_tuple_1)
            results.append(result_df)
        
        self.df_reports = delayed(pd.concat)(results)

        return self.df_reports

    def compute(self, additional_columns=pd. DataFrame()):
        """Initializes computation of task graph from set_up() on the Dask 
        Client. The results are saved as DataFrames and any additional columns 
        provided by the user are are added to the Dataframe result. The new 
        results table is returned.

        :param additional_columns: DataFrames provided by the user to be 
            appended to the result dataframe.
        :type additional_columns: Pandas Dataframe
        :return df: TYhe final results dataframe along with any additional 
        columns provided by the user.
        :rtype: Pandas Dataframe
        """
        summary_table = self.client.compute(self.df_reports)
        df = summary_table.result()
        df = df.reset_index(drop=True)
        if not additional_columns.empty:
            df = pd.concat([df, additional_columns], axis=1)

        return df

    def compute_report(self, 
                       dask_report="dask-report.html", 
                       summary_report="summary_report.csv", 
                       additional_columns=pd. DataFrame()):
        """Initializes computation of task graph from set_up() on the Dask 
        Client and creates a performance report for the computations. The 
        results are saved as DataFrames and any additional columns 
        provided by the user are are added to the Dataframe result. The new 
        results table is returned and the performance reports and the results 
        data frame are stored on disk.

        :param dask_report: Filename to save the performance report.
        :type dask_report: string
        :param summary_report: Filename to save the results dataframe as a .csv 
            file
        :type dask_report: string
        :return df: TYhe final results dataframe along with any additional 
        columns provided by the user.
        :rtype: Pandas Dataframe
        """
        # test if the filepath exist, if not create it
        time_stamp = strftime("%Y%m%d-%H%M%S")
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        # Compute tasks on cluster and save results

        with performance_report(self.output_path + "/" + f"{time_stamp}-" + dask_report):
            summary_table = self.client.compute(self.df_reports)
            df = summary_table.result()
            df = df.reset_index(drop=True)

            if not additional_columns.empty:
                df = pd.concat([df, additional_columns], axis=1)
            df.to_csv(self.output_path + "/" + f"{time_stamp}-" + summary_report)
        
        return df
