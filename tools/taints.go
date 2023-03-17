package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/DataDog/compute-go/table"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/planetlabs/draino/internal/cli"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
)

type taintCommandFlags struct {
	nodeName           string
	nodegroupName      string
	nodegroupNamespace string
	outputFormat       cli.OutputFormatType
	tableOutputParams  table.OutputParameters
}

var taintCmdFlags taintCommandFlags

func TaintCmd(mgr manager.Manager) *cobra.Command {
	taintCmd := &cobra.Command{
		Use:        "taint",
		SuggestFor: []string{"taint", "taints"},
		Args:       cobra.MaximumNArgs(1),
		Run:        func(cmd *cobra.Command, args []string) {},
	}
	taintCmd.PersistentFlags().VarP(&taintCmdFlags.outputFormat, "output", "o", "output format (table|json)")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodeName, "node-name", "", "", "name of the node")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodegroupName, "nodegroup-name", "", "", "name of the nodegroup")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodegroupNamespace, "nodegroup-namespace", "", "", "namespace of the nodegroup")
	cli.SetTableOutputParameters(&taintCmdFlags.tableOutputParams, taintCmd.PersistentFlags())

	nodeListerFilter := BuildNodeListerFilter(mgr.GetClient(), taintCmdFlags.nodeName, taintCmdFlags.nodegroupName, taintCmdFlags.nodegroupNamespace)

	listCmd := &cobra.Command{
		Use:        "list",
		SuggestFor: []string{"list"},
		Args:       cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodes, err := nodeListerFilter()
			if err != nil {
				return err
			}
			output, err := FormatNodesOutput(nodes)
			if err != nil {
				return err
			}
			fmt.Println(output)
			return nil
		},
	}

	deleteCmd := &cobra.Command{
		Use:        "delete",
		SuggestFor: []string{"delete"},
		Args:       cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodes, err := nodeListerFilter()
			if err != nil {
				return err
			}
			if len(nodes) == 0 {
				fmt.Printf("No node with NLA taint\n")
				return nil
			}

			output, err := FormatNodesOutput(nodes)
			if err != nil {
				return err
			}
			fmt.Printf("\nProcessing NLA candidate taint removal\n")
			for _, node := range nodes {
				t, f := k8sclient.GetNLATaint(node)
				if t == nil || !f {
					fmt.Printf("%v: no taint found\n", node.Name)
					continue
				}
				if t.Value != k8sclient.TaintDrainCandidate {
					fmt.Printf("%v: skipping taint %s\n", node.Name, t.Value)
					continue
				}

				if _, err := k8sclient.RemoveNLATaint(context.Background(), mgr.GetClient(), node); err != nil {
					fmt.Printf("%v: %v\n", node.Name, err)
					continue
				}
				fmt.Printf("%v: done\n", node.Name)
			}

			fmt.Println(output)
			return nil
		},
	}
	taintCmd.AddCommand(listCmd, deleteCmd)
	return taintCmd
}

func FormatNodesOutput(nodes []*v1.Node) (string, error) {
	if taintCmdFlags.outputFormat == cli.FormatJSON {
		b, err := json.MarshalIndent(nodes, "", " ")
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	table := table.NewTable([]string{
		"Name", "NodegroupNamespace", "Nodegroup", "NLATaint",
	},
		func(obj interface{}) []string {
			node := obj.(*v1.Node)
			ng, ngNs := NGValues(node)
			tValue := ""
			if taint, _ := k8sclient.GetNLATaint(node); taint != nil {
				tValue = taint.Value
			}
			return []string{
				node.Name,
				ngNs,
				ng,
				tValue,
			}
		})

	for _, n := range nodes {
		table.Add(n)
	}
	taintCmdFlags.tableOutputParams.Apply(table)
	buf := bytes.NewBufferString("")
	err := table.Display(buf)
	return buf.String(), err
}

func NGValues(node *v1.Node) (name, ns string) {
	if node.Labels == nil {
		return "", ""
	}
	return node.Labels[kubernetes.LabelKeyNodeGroupName], node.Labels[kubernetes.LabelKeyNodeGroupNamespace]
}

func BuildNodeListerFilter(kclient client.Client, nodeName string, ngName string, ngNamespace string) func() ([]*v1.Node, error) {
	if nodeName != "" {
		return func() ([]*v1.Node, error) {
			var node v1.Node
			if err := kclient.Get(context.Background(), types.NamespacedName{Name: nodeName}, &node, &client.GetOptions{}); err != nil {
				return nil, err
			}
			if _, f := k8sclient.GetNLATaint(&node); !f {
				return nil, nil
			}

			ng, ngNs := NGValues(&node)
			if ngNamespace != "" && ngNamespace != ngNs {
				return nil, nil
			}
			if ngName != "" && ngName != ng {
				return nil, nil
			}
			return []*v1.Node{&node}, nil
		}
	}
	return func() ([]*v1.Node, error) {
		var nodes v1.NodeList
		if err := kclient.List(context.Background(), &nodes, &client.ListOptions{}); err != nil {
			return nil, err
		}
		var result []*v1.Node
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if _, f := k8sclient.GetNLATaint(node); !f {
				continue
			}

			ng, ngNs := NGValues(node)
			if ngNamespace != "" && ngNamespace != ngNs {
				continue
			}
			if ngName != "" && ngName != ng {
				continue
			}
			result = append(result, node)
		}
		return result, nil
	}

}
