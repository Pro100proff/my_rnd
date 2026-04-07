package com.platform.archive.maintenance.rotation;

import java.util.List;

public record DropPartitionsAction(List<String> partitionPaths) implements RotationAction {
}
