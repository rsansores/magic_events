//! A field whose name is a keyword — `type`, written `r#type` — is named in a
//! template either way.

use magic_events::Event;
use serde::Serialize;

#[derive(Serialize, Event)]
#[event(type = "device.changed", version = 1, subject = "devices.{type}.{r#loop}", ce_subject = "{r#type}")]
pub struct DeviceChanged {
    pub r#type: String,
    pub r#loop: u32,
}

#[derive(Serialize, Event)]
pub enum Devices {
    #[event(type = "device.moved", version = 1, subject = "devices.{type}.moved")]
    Moved { r#type: String },
}

fn main() {
    fn takes<E: Event>(_: &E) {}
    takes(&DeviceChanged { r#type: "pump".into(), r#loop: 1 });
    takes(&Devices::Moved { r#type: "pump".into() });
}
