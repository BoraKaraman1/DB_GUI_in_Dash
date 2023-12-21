import requests
import json
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State
import tokens
from datetime import datetime


# get auth tokens
DENV = tokens.DBXEnvironment("dev")

# assign the token and host to variables
DOMAIN = DENV.host
TOKEN = DENV.token

# API endpoints for listing jobs
job_url = DOMAIN + '/api/2.0/jobs/list'
jobRuns_url = DOMAIN + '/api/2.0/jobs/runs/list'

# Prepare the header for authentication
headers = {
    'Authorization': f'Bearer {TOKEN}'
}

# initialize the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])



# get the runs of a job
def jobRuns(job_id):

  """
  Get the runs of a job from the API.
    
  Args:
  job_id (str): The ID of the job for which runs are to be fetched.

  Returns:
  list: A list of runs for the given job, or an empty list if the request fails.
  """ 
  
  payload = {'job_id': job_id}
  responseR = requests.get(jobRuns_url, headers = headers, params = payload)
  if responseR.status_code == 200:
        return responseR.json().get('runs', [])
  else:
        print(f"Failed to list runs for job {job_id}: {responseR.status_code} - {responseR.text}")
        return []



# get jobs
def list_jobs():

    """
    Fetch the list of jobs from the API.
    
    Returns:
    list: A list of jobs, or None if the request fails.
    """
    response = requests.get(job_url, headers=headers)
    if response.status_code == 200:
        # Parse the job list
        jobs = response.json().get('jobs', [])
        return jobs
           
    else:
        return None



def calc_running_job_dur(start_time):

    """
    Calculate the duration of a running job based on its start time.

    Args:
    start_time (int): The start time of the job in epoch milliseconds.

    Returns:
    int: The duration of the job in whole seconds, or None if start_time is not provided.
    """

    if start_time:
        # Convert start_time from epoch milliseconds to a datetime object
        start_time = datetime.fromtimestamp(start_time / 1000)

        # Get the current time
        current_time = datetime.now()

        # Calculate the duration
        duration = current_time - start_time

        # Duration in seconds
        duration_in_seconds = int(duration.total_seconds())

        return duration_in_seconds  
    else:
        return None



# Fetches only the last run of a job. Greatly accelerates the initialization
def lastRun(job_id):

    """
    Fetches the most recent run of a specified job.

    Args:
    job_id (str): The ID of the job for which the last run is to be fetched.

    Returns:
    dict: A dictionary containing details of the last run, or None if no runs are found.
    """

    payload = {'job_id': job_id, 'active_only': False, 'limit': 1}  # Fetch only the most recent run
    responseR = requests.get(jobRuns_url, headers=headers, params=payload)
    if responseR.status_code == 200:
        runs = responseR.json().get('runs', [])
        if runs:
            run = runs[0]
            
            start_time = run.get('start_time')
            end_time = run.get('end_time')
            duration_ms = end_time - start_time

            # Format start time
            run['formatted_start_time'] = datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if start_time else 'N/A'
            # Format duration
            run['formatted_duration'] = duration_ms // 1000 
            
            # Include result state
            run['result_state'] = run.get('state', {}).get('result_state', 'N/A')
            # Include life cycle state
            run['lifecycle_state'] = run.get('state', {}).get('life_cycle_state', 'N/A')

            # The duration is displayed as 0 if the job is pending and if the job is running elapsed time at the moment of refresh is shown
            
            if run['lifecycle_state'] == 'RUNNING' or run['lifecycle_state'] == 'PENDING': run['formatted_duration'] = calc_running_job_dur(start_time)

            if run['formatted_duration'] == 1:
              run['formatted_duration'] = str(run['formatted_duration']) + ' second' if duration_ms else 'N/A'  # Convert ms to seconds

            else:
              run['formatted_duration'] = str(run['formatted_duration']) + ' seconds' if duration_ms else 'N/A'  # Convert ms to seconds

            
            # aids the next function by assingning a color depending on the result
            result_color = '#008f00' if run['result_state'] == 'SUCCESS' else 'red' if run['result_state'] == 'FAILED' else '#ed9a00' if run['lifecycle_state'] == 'RUNNING' \
             else 'red' if run['result_state'] == "MAXIMUM_CONCURRENT_RUNS_REACHED" else 'gray'
            
            run['result_color'] = result_color

            # Depending on the specific lifecycle and result states, determines which of the attributes will be displayed in bold
            if run['lifecycle_state'] == 'RUNNING' or run['lifecycle_state'] == 'PENDING':
                run['lifecycleFont'] = 'bold'
                run['resultFont'] = 'normal'
            else:
                run['lifecycleFont'] = 'normal'
                run['resultFont'] = 'bold'

            
            return run
        else:
            return None



# Function to create rows of cards
def create_card_rows(jobs, cards_per_row=99):

    """
    Creates rows of cards displaying job details.

    Args:
    jobs (list): A list of job dictionaries.
    cards_per_row (int): Number of cards to display per row.

    Returns:
    list: A list of dbc.Row objects, each containing a row of cards.
    """


    # Dictionary to hold the last run details for each job
    last_run_details = {job['job_id']: lastRun(job['job_id']) for job in jobs}
    # cards are generated in rows
    rows = []
    for i in range(0, len(jobs), cards_per_row):
        row_jobs = jobs[i:i+cards_per_row]
        row = dbc.Row(
            [dbc.Card(
                    dbc.CardBody([
                        html.H5(job.get('settings', {}).get('name', f"Job ID: {job['job_id']}"), className="card-title",
                                style={'color': last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'gray',
                                      'font-size': '25px', 'margin-bottom' : '18px', 'text-align' : 'center'}),       
                        
                        html.P(last_run_details[job['job_id']].get('lifecycle_state', 'N/A') if last_run_details[job['job_id']] else 'N/A', 
                               style= {'color': last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'gray',
                                        'font-size': '17px',
                                        'font-weight': last_run_details[job['job_id']].get('lifecycleFont', 'N/A') if last_run_details[job['job_id']] else 'normal',
                                        'margin-bottom' : '0px'}),

                        html.P(f"State", style= {'color': 'gray', 'font-size': '10px', 'font-weight': 'normal', 'margin-top': '0px', 'margin-bottom' : '10px'}),                
                        
                        html.P(last_run_details[job['job_id']].get('result_state', 'N/A') if last_run_details[job['job_id']] else 'N/A', 
                               style= {'color': last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'gray',
                                        'font-size': '17px',
                                        'font-weight': last_run_details[job['job_id']].get('resultFont', 'N/A') if last_run_details[job['job_id']] else 'bold',
                                        'margin-bottom' : '0px'}),

                        html.P(f"Result", style= {'color': 'gray', 'font-size': '10px', 'font-weight': 'normal', 'margin-top': '0px', 'margin-bottom' : '15px'}),  

                        html.P(f"Last Run Start Time:", style= {'font-size': '11px', 'margin-bottom' : '0px'}),

                        html.P(last_run_details[job['job_id']].get('formatted_start_time', 'N/A') if last_run_details[job['job_id']] else 'N/A', 
                               style= {'font-size': '11px', 'margin-top' : '0px'}),

                        html.P(f"Duration: {last_run_details[job['job_id']].get('formatted_duration', 'N/A') if last_run_details[job['job_id']] else 'N/A'}",
                               style= {'font-size': '11px'}),

                        dbc.Button("Show All Runs", id={'type': 'show-all-runs-button', 'index': idx}, n_clicks=0, className="button-click-effect")

                    ]),
                    style={"width": "16rem", 
                           "height": "18rem",
                           "margin-right": "3rem",
                           "margin-left": "1.2rem",
                           "margin-bottom": "1rem",
                           "border": "3px solid",
                           "border-color": last_run_details[job['job_id']].get('result_color', 'N/A') if last_run_details[job['job_id']] else 'gray',
                           "backgroundColor" : "#ecf0f2",
                           "position" : "relative"}
                ) for idx, job in enumerate(row_jobs, start=i)]
            ) 
        rows.append(row)
    return rows



# initialises the section for the list of runs
def initRunSection():
    return html.P(f"No Jobs Were Selected", style= {"font-size": "15px", 'margin-top': "10px", 'margin-left': "10px"})



# creates the list with checkboxes on the pop-up window 
def listOfJobsW(jobs, filter_text=""):

    """
    Creates a list with checkboxes for each job.

    Args:
    jobs (list): A list of job dictionaries.
    filter_text (str): Text to filter jobs by name.

    Returns:
    list: A list of dbc.Row objects, each containing a checkbox for a job.
    """

    filtered_jobs = [job for job in jobs if filter_text.lower() in job.get('settings', {}).get('name', f"Job ID: {job['job_id']}").lower()]
    rows = []
    for job in filtered_jobs:
        job_name = job.get('settings', {}).get('name', f"Job ID: {job['job_id']}")
        checkbox = dbc.Checkbox(
            id={"type": "dynamic-checkbox", "index": job['job_id']},
            label=job_name,
            persistence=True,
            value = False,
            className = "custom-checkbox"
        )
        row = dbc.Row(
            dbc.Col(html.P(checkbox, className = "form-check")),
            className="mb-2",  # Add margin for spacing between rows
            style = {"margin-top": "10px"}
        )
        rows.append(row)
    return rows

isRorL = 0

# App layout
app.layout = html.Div([
    html.H1(html.Span('Job Details and Runs', style={'margin-left': '10px'}), 
            style = {"color" : "#ecf0f2", "backgroundColor" : "#5a5c5f", "padding-bottom": "10px"}),
    html.Div(id='job-cards', style={'margin-top': '10px'}),

    # Interval for auto update of cards. It is disabled but left in the program in case a future developer decides to use it again. 
    dcc.Interval(
        id='interval-component',
        interval= 9999999999999,
        n_intervals=0,
        disabled= True
    ),
    dbc.Button(html.Img(src="/assets/configure.png", style={'height':'22px', 'width':'22px'}), id= 'configure-button', n_clicks=0, className="buttonC"),
    dbc.Button(html.Img(src="/assets/refresh.png", style={'height':'22px', 'width':'22px'}), id= 'refresh-button', n_clicks=0, className="buttonR"),
    dbc.Button(html.Img(src="/assets/connection.png", style={'height':'22px', 'width':'22px'}), id= 'AdChange-button', n_clicks=0, className="buttonAdC"),

    html.Div(id='button-click-output', children = initRunSection()),
    dcc.Store(id='checkbox-states', storage_type='local'),
    dcc.Store(id = 'jobs', storage_type= 'local'),
    dcc.Store(id = 'selected_jobs', storage_type='local'),

    # Stores that future address change functionality will use
    dcc.Store(id = 'hostAddress', storage_type= 'local'),
    dcc.Store(id = 'token', storage_type='local'),

    # Store for tracking if the page was refreshed or loaded. Used by a callback.
    dcc.Store(id = 'isRorL', storage_type='memory', data = isRorL),
    
    # Modal for job selection 
    dbc.Modal(
            [
                dbc.ModalHeader(html.H4("Jobs to Display", style={'font-size': '25px', 'color': '#ecf0f2'}),
                   style={'backgroundColor': '#5a5c5f'}),
                dbc.ModalBody([
                   dbc.Input(id="job-search-bar", placeholder="Search jobs...", type="text", style={'margin-bottom' : '12px'}),
                   dbc.Button("See All", id= "DSAll", className= "buttonSeeAll"),
                   dbc.Col(html.Div(id="job-list-container"))
                 ], style = {"backgroundColor" : "#ecf0f2"}
                ),
                dbc.ModalFooter(
                    dbc.Button("Apply", id="close", className="buttonConfClose"),
                    style = {"backgroundColor" : "#ecf0f2"}
                ),
            ],
            id="configure-window",
        ),
    
    # Modal for address change
    dbc.Modal(
            [
                dbc.ModalHeader(html.H4("Address Change", style={'font-size': '25px', 'color': '#ecf0f2'}),
                   style={'backgroundColor': '#5a5c5f'}),
                dbc.ModalBody([
                   dbc.Input(id="host-entry-bar", placeholder=DOMAIN, type="url", style={'margin-bottom' : '12px'}),
                   dbc.Input(id="token-enty-bar", placeholder="Enter token...", type="password", style={'margin-bottom' : '12px'})
                 ], style = {"backgroundColor" : "#ecf0f2"}
                ),
                dbc.ModalFooter(
                    dbc.Button("Change", id="close2", className="buttonConfClose"),
                    style = {"backgroundColor" : "#ecf0f2"}
                ),
            ],
            id="addressChange-window",
        )    
    ], style = {"backgroundColor" : "#ecf0f2"}
)



# creates the list that appears after the user clicks a show all runs button
def create_run_table(runs, job_name):

    """
    Creates a table of job runs for display.

    Args:
    runs (list): A list of runs to display.
    job_name (str): The name of the job associated with the runs.

    Returns:
    list: A table, each row representing a job run.
    """

    table_rows = []
    for run in runs:
        # Convert start_time to readable format
        start_time = run.get('start_time')
        if start_time:
            start_timeR = datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            start_timeR = 'N/A'

        # Calculate run duration
        end_time = run.get('end_time')  
        duration_ms = end_time - start_time
        
        duration = duration_ms // 1000 
            
        # Determine color for result state
        result_state = run.get('state', {}).get('result_state', 'N/A')
        lifecycle_state = run.get('state', {}).get('life_cycle_state', 'N/A')

        if result_state == 'MAXIMUM_CONCURRENT_RUNS_REACHED': result_state = 'MAX_CONC_RUNS'
        result_color = '#008f00' if result_state == 'SUCCESS' else 'red' if result_state == 'FAILED' else 'red' if result_state == 'MAX_CONC_RUNS' else 'black'
        lifecycle_color = '#ed9a00' if lifecycle_state == 'RUNNING' else 'black'

        
        # The duration is displayed as 0 if the job is pending and if the job is running elapsed time at the moment of refresh is shown
        
        if lifecycle_state == 'RUNNING' or lifecycle_state == 'PENDING' :  duration = calc_running_job_dur(start_time) 

        if duration == 1:
          duration = str(duration) + ' second' if duration_ms else 'N/A'  # Convert ms to seconds

        else:
          duration = str(duration) + ' seconds' if duration_ms else 'N/A'  # Convert ms to seconds

        
        # Url is readied here for better readability of the code 
        linkRP = run.get('run_page_url', 'N/A')

        # Create a table row for each run with border and colored result state
        row = html.Tr([
          html.Td(job_name, style={'font-size': '14px'}),
          html.Td(str(run.get('run_id', 'N/A')), style={'font-size': '14px'}),
          html.Td(start_timeR, style={'font-size': '14px'}),
          html.Td(duration, style={'font-size': '14px'}),
          html.Td(lifecycle_state, style={'color': lifecycle_color, 'font-size': '14px'}),
          html.Td(result_state, style={'color': result_color, 'font-size': '14px'}),
          html.Td(html.A("Go to page", href=linkRP), style={'font-size': '14px'})
        ])
        table_rows.append(row)

    table_body = [html.Tbody(table_rows)]
    return table_body



# Callback for handling button click
@app.callback(
    Output('button-click-output', 'children'),
    [Input({'type': 'show-all-runs-button', 'index': dash.ALL}, 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [State({'type': 'show-all-runs-button', 'index': dash.ALL}, 'n_clicks'),
     State('selected_jobs', 'data')]
)

def display_click(button_clicks, n_intervals, button_states, selected_jobs):
    ctx = dash.callback_context
    # Determine what triggered the callback
    if ctx.triggered and ctx.triggered[0]['prop_id'].split('.')[0] != 'interval-component':
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        index = json.loads(button_id)['index']
        # Ensure the button was actually clicked
        if button_states[index] > 0:
            job_id = selected_jobs[index]['job_id']
            runs = jobRuns(job_id)
            job_name = selected_jobs[index].get('settings', {}).get('name', f"Job ID: {selected_jobs[index]['job_id']}")

            table_header = [
                html.Thead(html.Tr([
                  html.Th("Job Name", style={'width': '10%', 'font-size': '14px'}),
                  html.Th("Run ID", style={'width': '15%', 'font-size': '14px'}),
                  html.Th("Start Time", style={'width': '15%', 'font-size': '14px'}),
                  html.Th("Duration", style={'width': '10%', 'font-size': '14px'}),
                  html.Th("Lifecycle State", style={'width': '15%', 'font-size': '14px'}),
                  html.Th("Result State", style={'width': '15%', 'font-size': '14px'}),
                  html.Th("Link", style={'width': '20%', 'font-size': '14px'})
                ]))
            ]
            
            table_body = create_run_table(runs, job_name)
              
            full__table = dbc.Table(table_header + table_body, bordered=True, hover=True, responsive=True, striped=True, className="runs-table custom-table-width")

            output_layout = [html.Div([
             html.P(f"Runs for the Job: {job_name}", 
                   style={'color': 'black', 'font-size': '30px', 'margin-left': "10px", 'margin-top': "25px"}),

             html.Div(full__table)
            ])]


            return output_layout
        return dash.no_update
    return dash.no_update
       
    
    
# Callback to update cards when the refresh button is clicked or each time the selection is changed
@app.callback(
    [Output('job-cards', 'children'),
    Output('selected_jobs', 'data'),
    Output('jobs','data'),
    Output('isRorL', 'data')],
    [Input('interval-component', 'n_intervals'),
    Input('checkbox-states', 'data'),
    Input("refresh-button", "n_clicks")],
    [State('jobs', 'data'),
    State('isRorL', 'data')]
)

# upgrade cards
def update_cards(n, checkbox_states, refreshB, jobs, isRorL):
    
    context = dash.callback_context

    if not context.triggered:
        # On initial load
        jobs = list_jobs()
    elif context.triggered[0]['prop_id'] == 'checkbox-states.data' and isRorL == 0:
         #If triggered by page load and refresh. RorL, being stored in memory, is made 0 in every refresh and load. This changes it to 1 so further updates of checkboxes don't trigger it.
        jobs = list_jobs()
        isRorL = 1
    elif context.triggered[0]['prop_id'] == 'refresh-button.n_clicks':
        # If triggered by refresh button
        jobs = list_jobs()

    
     #Filter the jobs based on the checkbox states
    if checkbox_states is None: # if the list is not initialized display all
          selected_jobss = jobs
    else:
          selected_jobss = [job for job in jobs if checkbox_states.get(str(job['job_id']))]
          if selected_jobss == []: #if none are selected display all
              selected_jobss = jobs         
    return create_card_rows(selected_jobss), selected_jobss, jobs, isRorL



#window opener for job selection
@app.callback(
    Output("configure-window", "is_open"),
    [Input("configure-button", "n_clicks"), Input("close", "n_clicks")],
    [State("configure-window", "is_open")],
)

def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open


#window opener for address change 
@app.callback(
    Output("addressChange-window", "is_open"),
    [Input("AdChange-button", "n_clicks"), Input("close2", "n_clicks")],
    [State("addressChange-window", "is_open")],
)

def toggle_modal2(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

#updates the storage that holds which jobs were selected on the pop-up window. Only called after the button on the window is clicked
@app.callback(
    Output('checkbox-states', 'data'),
    Input("close", "n_clicks"),
    [State('jobs', 'data'),
    State({'type': 'dynamic-checkbox', 'index': dash.ALL}, 'value'),
    State({'type': 'dynamic-checkbox', 'index': dash.ALL}, 'id')],
    prevent_initial_call=True
)

def update_checkbox_states(close_clicks, jobs, checked_states, indexes):
    if close_clicks:
        updated_data = {}
        for idx, checked in enumerate(checked_states):
            if idx < len(jobs):
             job_id = indexes[idx].get('index', 'N/A')
             updated_data[str(job_id)] = checked
        return updated_data
    else:
        return dash.no_update
    


# Search function. Also upgrades the list on the pop-up window each time jobs are refreshed. 
@app.callback(
    Output("job-list-container", "children"),
    [Input("job-search-bar", "value"),
    Input('jobs', 'data')]
     
)
def update_job_list(search_value, jobs):
    if not search_value:
        # If the search bar is empty, show all jobs
        return listOfJobsW(jobs)
    else:
        # Filter the jobs based on the search input
        return listOfJobsW(jobs, filter_text=search_value)
    


# Callback and function for the "See All" button on the modal
@app.callback(
    
    Output({'type': 'dynamic-checkbox', 'index': dash.ALL}, 'value'),
    Input("DSAll", "n_clicks"),
    State({'type': 'dynamic-checkbox', 'index': dash.ALL}, 'value'),
    prevent_initial_call = True
)

def deselectAll(DSAll_clicks, checkboxes):
    if DSAll_clicks:
     deselected_checkboxes = []
     for idx in range(len(checkboxes)):
        deselected_checkboxes.append(False)

     return deselected_checkboxes
    else:
     return dash.no_update



if __name__ == '__main__':
    app.run_server(debug=False)