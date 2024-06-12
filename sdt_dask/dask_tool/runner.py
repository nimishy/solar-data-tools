"""
This module provides a class to run the SolarDataTools pipeline on a Dask 
Cluster. It takes a Dask Client as input and use the DataPlug to retrieve 
datasets to run with the pipeline on the Dask Client after setting up the task 
graph.

See the README for more information.

"""
import os
import pandas as pd
from dask import delayed
from time import strftime
from dask.distributed import performance_report
from solardatatools import DataHandler

class Runner:
    """A class to run the SolarDataTools pipeline on a Dask cluster. Handles 
    invalid data keys and failed datasets.

    :param client: The initialized Dask Client Object to submit the task graph 
        for computations.
    :type client: Dask Client Object
    """
    
    def __init__(self, client):
        self.client = client

    def set_up(self, KEYS, dataplug, **kwargs):
        """Function to retrieve datasets using the keys and DataPlug. Sets up 
        the pipeline on the Dask Client by creating a task graph.

        Calls run_pipeline functions in a for loop over the keys
        and combines results into a DataFrame.

        :param keys: List of tuples to used to access PV datasets.
        :type keys: list
        :param dataplug: DataPlug object to get datasets associated to its 
            matching keys.
        :type dataplug: DataPlug Object
        :param kwargs: Additional arguments for SolarDataTools run_pipeline.
        :type kwargs: dict
        :return df_reports: Returns a a delayed object consisting of the task 
            graph for computation.
        :rtype: Dask Delayed Object
        """

        def get_data(key):
            """Creates a DataFrame for a key that includes the errors for the
            functions performed on the file and its DataHandler.

            :param key: The key combination used to access a file.
            :type key: tuple
            :return: Returns the DataFrame for the key and its DataHandler.
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
            # DataFrame
            data_dict = {**key_dict, **errors}
            data_df = pd.DataFrame.from_dict(data_dict)

            try:
                # Reads dataset file and creates DataFrame, Here the function 
                # reads data into pandas and python instead of using Dask to 
                # store the data in memory for further computation **.
                df = dataplug.get_data(key)
                dh = DataHandler(df)
                return (data_df, dh)
            except Exception as e:
                data_df["get_data error"] = str(e)
                return (data_df,)
            

        def run_pipeline(data_tuple, **kwargs):
            """Function runs the pipeline and appends the results to the
            DataFrame. The function also stores the exceptions for the function
            call into its respective errors.

            :param data_tuple: The tuple consists of the DataFrame and 
                DataHandler.
            :type data_tuple: tuple
            :param kwargs: The keyword arguments passed to the DataHandler's
                run_pipeline.
            :type kwargs: dict
            :return: Tuple containing key's pipeline results DataFrame and 
                DataHandler.
            :rtype: tuple
            """

            # Assigns the DataFrame, the first element of the tuple.
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
                        data_df["run_loss_analysis error"] = "The length of data is less than or equal to 1 year, loss analysis will fail thus is not performed."
                        data_df["run_loss_analysis_report error"] = "Loss analysis is not performed"

                except Exception as e:
                    data_df["run_pipeline error"] = str(e)
                    error = "Failed because of run_pipeline error"
                    data_df["run_loss_analysis error"] = error
                    data_df["run_pipeline_report error"] = error
                    data_df["run_loss_analysis_report error"] = error


            # Gets the run_pipeline report and appends it to the DataFrame as
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
            the loss report results by appending it to the key DataFrame. All
            errors are assigned to the key DataFrame in error reports.

            :param data_tuple: A tuple containing the key DataFrame and the
                datahandler object.
            :type data_tuple: tuple
            :return: key DataFrame with appended reports and assigned error 
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

    def compute(self, 
                report=False,
                output_path="../results/",
                dask_report="dask_report", 
                summary_report="summary_report", 
                additional_columns=pd. DataFrame()):
        """Initializes computation of task graph from set_up() on the Dask 
        Client and generates a performance report when requested by the user. 
        The results are saved as DataFrames and any additional columns provided 
        by the user are are added to the Dataframe result. The new results table
        is returned.
        
        :param report: Flag to produce performance report for computations.
        :param report: bool
        :param output_path: Directory path to save reports and results 
            DataFrame.
        :type output_path: string
        :param dask_report: Filename to save the performance report.
        :type dask_report: string
        :param summary_report: Filename to save the results DataFrame as a .csv 
            file.
        :type dask_report: string
        :param additional_columns: DataFrames provided by the user to be 
            appended to the result DataFrame.
        :type additional_columns: Pandas Dataframe
        :return df: The final results DataFrame along with any additional 
            columns provided by the user.
        :rtype: Pandas Dataframe
        """
        # Compute task graph and save results DataFrame and performance report.
        if report:
            # checks and creates result directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)

            # test if the file path exist, if not create it
            time_stamp = strftime("%Y%m%d-%H%M%S")
            
            # Compute tasks on cluster and save results as a single DataFrame
            with performance_report(output_path + f"{time_stamp}_" + 
                                    dask_report + '.html'):
                summary_table = self.client.compute(self.df_reports)
                df = summary_table.result()
                df = df.reset_index(drop=True)

                # Add additional columns provided by user and save as CSV file
                if not additional_columns.empty:
                    df = pd.concat([df, additional_columns], axis=1)
                df.to_csv(output_path + "/" + f"{time_stamp}_" 
                          + summary_report + '.csv')
                
        # Compute task graph and return DataFrame
        else:
            summary_table = self.client.compute(self.df_reports)
            df = summary_table.result()
            df = df.reset_index(drop=True)
            if not additional_columns.empty:
                df = pd.concat([df, additional_columns], axis=1)

        return df
