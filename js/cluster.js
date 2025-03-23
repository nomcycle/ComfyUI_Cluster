import { app } from "../../scripts/app.js";
app.registerExtension({ 
	name: "comfyui.cluster",
	async setup() { 
		const menu = document.querySelector(".comfyui-menu-right");
		let cmGroup = new (await import("../../scripts/ui/components/buttonGroup.js")).ComfyButtonGroup(
			new(await import("../../scripts/ui/components/button.js")).ComfyButton({
				icon: "play",
				action: async () => {
					// app.queuePrompt(0, 1);
					const prompt = await app.graphToPrompt()
					try {
						const response = await fetch('/cluster/queue', {
							method: 'POST',
							headers: {
								'Content-Type': 'application/json'
							},
							body: JSON.stringify(prompt)
						});

						if (!response.ok) {
							throw new Error(`HTTP error! status: ${response.status}`);
						}
					} catch (error) {
						console.error('Error queueing to cluster:', error);
					}
					console.log(prompt);
				},
				tooltip: "Queue workflow to cluster.",
				content: "Queue Cluster",
				classList: "comfyui-button comfyui-menu-mobile-collapse primary"
			}).element
		);

		// app.actionbar.queuebutton
		app.menu?.settingsGroup.element.before(cmGroup.element);
	},
})