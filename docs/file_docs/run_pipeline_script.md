# Own docs regarding run_pipeline.py script (CLI commands using argparse)
---

## Notes och insikter kring run_pipeline scriptet.
Hela syftet med `run_pipeline.py` är att ha ett script med egna CLI kommandon för att enkelt kunna köra vilka script jag nu vill. Detta för att inte behöva memorera vad script heter, vilken pathing mina olika script har utan samla allting på en plats med liknande kommandon.

Ett exempel på hur hela pipelinen ska köras från bronze till silver till gold layers aggregeringar är något i stil med:  
`uv run python -m scripts.run_pipeline --layer all`

