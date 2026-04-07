package com.platform.archive.maintenance.rotation;

public sealed interface RotationAction permits DropPartitionsAction, FilterContentAction, SampleContentAction, SwitchDirectoryAction {
}
