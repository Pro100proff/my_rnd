package com.platform.archive.maintenance.rotation;

public record SwitchDirectoryAction(String oldDirectory, String newDirectory, int graceMinutes) implements RotationAction {
}
