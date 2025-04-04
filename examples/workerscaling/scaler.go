/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/controller"
)

// QueueBasedScaler 是一个自定义工作器伸缩器
// 根据队列长度和性能指标动态调整工作器数量
type QueueBasedScaler struct {
	// 每个工作器处理的目标队列项数
	TargetItemsPerWorker int
	// 最大增长比例 (0.5 = 50%)
	MaxGrowthRatio float64
	// 最大减少比例 (0.2 = 20%)
	MaxReductionRatio float64
	// 日志函数
	Log func(string, ...interface{})
}

// EvaluateWorkerCount 实现controller.WorkerScaler接口
func (s *QueueBasedScaler) EvaluateWorkerCount(ctx context.Context, info controller.WorkerScalingInfo) (int, error) {
	// 队列为空，降至最小值
	if info.QueueLength == 0 {
		return info.MinWorkers, nil
	}

	// 应用默认值
	itemsPerWorker := s.TargetItemsPerWorker
	if itemsPerWorker <= 0 {
		itemsPerWorker = 3
	}

	maxGrowthRatio := s.MaxGrowthRatio
	if maxGrowthRatio <= 0 {
		maxGrowthRatio = 0.5
	}

	maxReductionRatio := s.MaxReductionRatio
	if maxReductionRatio <= 0 {
		maxReductionRatio = 0.2
	}

	// 基于队列长度计算理想工作器数量
	desiredWorkers := (info.QueueLength + itemsPerWorker - 1) / itemsPerWorker

	// 确保在最小值和最大值范围内
	if desiredWorkers < info.MinWorkers {
		desiredWorkers = info.MinWorkers
	}
	if desiredWorkers > info.MaxWorkers {
		desiredWorkers = info.MaxWorkers
	}

	// 应用增长和减少限制，避免剧烈波动
	if desiredWorkers > info.CurrentWorkers {
		// 计算允许的最大增长
		maxGrowth := int(float64(info.CurrentWorkers) * maxGrowthRatio)
		if maxGrowth < 1 {
			maxGrowth = 1
		}

		// 限制增长以避免峰值
		if desiredWorkers > info.CurrentWorkers+maxGrowth {
			desiredWorkers = info.CurrentWorkers + maxGrowth
		}
		s.Log("扩展工作器",
			"当前", info.CurrentWorkers,
			"目标", desiredWorkers,
			"队列长度", info.QueueLength)
	} else if desiredWorkers < info.CurrentWorkers {
		// 计算允许的最大减少
		maxReduction := int(float64(info.CurrentWorkers) * maxReductionRatio)
		if maxReduction < 1 {
			maxReduction = 1
		}

		// 限制减少以避免快速缩减
		if desiredWorkers < info.CurrentWorkers-maxReduction {
			desiredWorkers = info.CurrentWorkers - maxReduction
		}
		s.Log("缩减工作器",
			"当前", info.CurrentWorkers,
			"目标", desiredWorkers,
			"队列长度", info.QueueLength)
	}

	return desiredWorkers, nil
}
