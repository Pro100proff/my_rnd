package com.platform.archive.maintenance.rotation;

public record FilterContentAction(String partitionPath, String condition) implements RotationAction {
}
