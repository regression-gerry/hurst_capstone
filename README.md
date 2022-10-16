# hurst_capstone

Python version: `3.9.13`

## Environment Management
To update/create environment.yml from current active conda environment, run:  
`conda env export | grep -v "^prefix: " > environment.yml`

To create environment from existing environment.yml file run:  
`conda env create -f environment.yml`

To update existing environment named "capstone" with environment.yml, run:  
`conda env update --name capstone --file environment.yml --prune`

To activate environment, run:  
`conda activate capstone`

For information on more conda commands, visit: [Conda Cheatsheet](https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf)

## Git Workflow

**DO NOT PUSH TO `main` branch**

Checkout your own branch from the main branch with:  
`git checkout -b {your_branch_name}`

To pull changes from main into your local branch, then push to your remote branch, run:  
1. `git checkout main`
2. `git pull`
3. `git checkout {your_branch_name}`
4. `git rebase main -i`
5. `git push -f`

## dao
- data pull code to be refactored

## notebooks
- test data pull code
- rescaled range analysis

## sqlite
- test on loading pickle files to sqlite db

## other folders
- misc items
