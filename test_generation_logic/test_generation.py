import random

def select_from_xor(selection):
    get_count = len(selection)
    choice_idx = selection[random.randrange(get_count)]
    return selection[choice_idx]


def get_arg2prompt(args_type, args_dict, required, p=0.5):
    
    # 1. Select an argument and confirm it exists
    if "XOR" in args_type:
        selection = []
        for arg_name, arg_data in args_dict[args_type].items():
            if not arg_data["associated_entity"]: # will have to modify this
                continue
            else:
                selection.append(arg_name)
        if not selection:
            if required: return -2, "" # No selection of required field was possible
            else: return -1, "" # No selection of non-required field was possible
        
        selected_arg = select_from_xor(selection)
        selected_arg_data = args_dict[args_type][selected_arg]
    
    else: # So an argument has to be inlcuded
        selected_arg = args_type
        selected_arg_data = args_dict[selected_arg]
        if required:
            if not selected_arg_data["associated_entity"]: # will have to modify this
                return -2, "" # No selection of required field was possible
        else: # not required
            if random.random() < p:
                return 0, "" # Randomly decided to to include non-required argument
            else:
                if not selected_arg_data["associated_entity"]: # will have to modify this
                    return -1, "" # No selection of non-required field was possible
    
    
    # 2. Generate a prompt of the argument
    queriable_fields = list(selected_arg_data["queriable_fields"].keys())
    selected_field = select_from_xor(queriable_fields)
    entities_ids = # list of ids of required ids just need to query DB for this.
    selected_entity = select_from_xor(entities_ids)
    entity_field = str(selected_entity.selected_field) # again query from the DB. This will have to be changed to account for differnet possible data types. Assume they can be queried from the prompt generation encoding.
    prompt = selected_arg_data["queriable_fields"][selected_field]["prompt"] + entity_field 
    return 0, prompt
    
    
   
def create_aciton_prompt(action, p):
    prompt = ""
    
    required_args = action["required_args"]
    for required_arg_type in required_args:
        status, prompt_addition = get_arg2prompt(required_arg_type, required_args, required=True)
        
        if status == -2:
            raise ValueError("No entity for a required feild exists.")
        else:
            prompt += prompt_addition
    
    nonrequired_args = action["nonrequired_args"]
    for nonrequired_arg_type in action["nonrequired_args"]:
        status, prompt_addition = get_arg2prompt(nonrequired_arg_type, nonrequired_args, required=False)
        
        if status == -2:
            raise ValueError("No entity for a required feild exists.")
        else:
            prompt += prompt_addition
    
    return prompt


######### SAMPLE PROMPT ENCODING STRUCTURE ################
# I will only add neccessary fields for the operation (assumingly of the algorithm above)
action = {
    "action_name": "action_name",
    "required_args": {
        "XOR_1": { # OR actually any name with XOR. Those are going to represent arguments where only one of the arguments in the list can be created. Use multiple XOR fields with different names to get multiple selections
            "arg_name1": {
                "associated_entity": "associated_enttity_from_the_DB"
                "queriable_fields": {
                    # From qieroable fields only one is going to be selected randomly
                    "queriable_field_name1": {
                        "type": "string"
                        "prompt": "sample prompt that will be added"
                    },
                    "queriable_field_name2": {
                        "type": "JSON"
                        "prompt": "sample prompt that will be added"
                    }
                    # ....
                } 
            },
            "arg_name2": {
                # Repeat the same fields as for arg_name1    
            }
            # .....
        },
        "arg_name3": {
            # Repeat the same fields as for arg_name1 and arg_name2
        }
        "arg_name4": {
            # Repeat the same fields as for arg_name1, arg_name2, arg_name3
        }
    }
    "nonrequired_args": {
        # The same structure as for "required_args"
    }
}
