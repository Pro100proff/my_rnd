package com.platform.archive.maintenance.rotation;

public record SampleContentAction(String partitionPath, String condition, double keepRatio) implements RotationAction {
}
