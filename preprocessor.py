import os
from time import sleep, time
import geopandas as gpd
import fiona
from tqdm import tqdm
from pytictoc import TicToc
t = TicToc()

def run():
    print('Running preprocessor...')

    if os.path.exists('plss-monster'):
        print('PLSS data already loaded.')
        return

   

    plss_link = 'https://www.arcgis.com/sharing/rest/content/items/283939812bc34c11bad695a1c8152faf/data'

    # if plss.gdb is not in the current directory, download it
    if not os.path.exists('plss.gdb'):
        print('PLSS data not found in current directory.')
        # download the file if not already downloaded
        if not os.path.exists('plss.zip'):
            print('Downloading PLSS data...')
            os.system(f'wget {plss_link} -O plss.zip')
        if os.path.exists('plss.zip') and not os.path.exists('plss.gdb'):
            print('Unzipping PLSS data...')
            os.system('unzip plss.zip')
            # rename directory *plss.gdb to plss.gdb
            os.system('mv mv *plss.gdb plss.gdb')
            # remove the *_State_*.gdb directory
            os.system('rm -r *_State_*.gdb')
            # remove the zip file
            os.system('rm plss.zip')


    # fiona list layers in the PLSS data
    layers = fiona.listlayers('plss.gdb')

    # Print the list of layers
    print("Layers in the file geodatabase:")
    for layer in layers:
        print(layer)



    # use fiona to read layer 0 and get al the state attributes in the PLSS data
    # states = []
    # with fiona.open('plss.gdb', layer='PLSSTownship', where="STATEABBR = 'WY'") as src:
        
    #     for feature in src:
    #         state = feature['properties']['STATEABBR']
    #         if state not in states:
    #             states.append(state)



    # us_states_abbr =['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

    us_states_abbr = ['WY']
    if not os.path.exists('plss-monster_tmp'):
        os.makedirs('plss-monster_tmp')



    # build out state directories and save the townships
    for state in us_states_abbr:
        print(f'Loading Township data for {state}...')
        state_dir = f'plss-monster_tmp/{state}'
        plss = None
        if not os.path.exists(f'{state_dir}/townships.fgb'):

            plss = gpd.read_file('plss.gdb', driver='FileGDB', layer=2, where=f"STATEABBR = '{state}'")
        # if township data for the state exists, load it
        else:
            plss = gpd.read_file(f'{state_dir}/townships.fgb', driver='FlatGeobuf')
        # create a directory for the state if it does not exist
        
        if not os.path.exists(state_dir):
            os.makedirs(state_dir)
            # save townships to the state directory
        if not os.path.exists(f'{state_dir}/townships.fgb'):
            plss.to_file(f'{state_dir}/townships.fgb', driver='FlatGeobuf')

        # for each township, create a directory 
        for township in tqdm(plss['PLSSID']):
            township_dir = f'{state_dir}/{township}'
            if not os.path.exists(township_dir):
                os.makedirs(township_dir)
       


            

    state_sections = None

    # process the sections for the next township and save and exit

    # get states from directory structure
    states = os.listdir('plss-monster_tmp')
    # loop the states
    for state in states:
        print(f'Processing sections for {state}...')
        state_dir = f'plss-monster_tmp/{state}'
        # get townships from directory structure
        townships = os.listdir(state_dir)
        # remove the townships.fgb file
        if 'townships.fgb' in townships:
            townships.remove('townships.fgb')

        # loop the townships

        

        for township in tqdm(townships):

            township_dir = f'{state_dir}/{township}'
            # if sections are not saved, load them
            if not os.path.exists(f'{township_dir}/sections.fgb'):
                if state_sections is None:
                    # print('Loading state sections...')
                    state_sections = gpd.read_file('plss.gdb', driver='FileGDB', layer=0, where=f"PLSSID like '{state}%'")
                plss_sections = state_sections[state_sections['PLSSID'].str.startswith(township)]
                plss_sections.to_file(f'{township_dir}/sections.fgb', driver='FlatGeobuf')
            # if sections are saved, continue
            else:
                continue
        


    sleep(.1)





    # loop the states and load the PLSS data for each township
    for state in us_states_abbr:
        print(f'Loading Section data for {state}...')
        townships = os.listdir(state_dir)
        # remove the townships.fgb file
        if 'townships.fgb' in townships:
            townships.remove('townships.fgb')

        # loop the townships
  

        t.tic()

        #layout all the sections for the state
        if state_sections is None:
            state_sections = gpd.read_file('plss.gdb', driver='FileGDB', layer=0, where=f"PLSSID like '{state}%'")

        intersected= gpd.read_file('plss.gdb', driver='FileGDB', layer=1, where=f"PLSSID like '{state}%'")

        # get time elapsed
        t.toc("Intersected in")

        # group sections in batches of 100
        batch_size = 100
        section_bathes = []
        for i in range(0, len(state_sections), batch_size):
            batch = state_sections[i:i+batch_size]
            section_bathes.append(batch)

        # loop the batches
        print('Processing intersected by batch...')
        pbar = tqdm(len(state_sections))
        for batch in section_bathes:
            # load intersected by firstdiv ids
            

            pbar.update(len(batch))
            




    
                
                

