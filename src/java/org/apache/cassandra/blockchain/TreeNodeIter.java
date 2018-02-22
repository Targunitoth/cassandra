//Kopiert von https://github.com/kentliau/yet-another-tree-structure/blob/master/java/src/com/tree/TreeNodeIter.java

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.blockchain;

import java.util.Iterator;

public class TreeNodeIter implements Iterator<TreeNode>
{

    enum ProcessStages
    {
        ProcessParent, ProcessChildCurNode, ProcessChildSubNode
    }

    private TreeNode treeNode;

    public TreeNodeIter(TreeNode treeNode)
    {
        this.treeNode = treeNode;
        this.doNext = ProcessStages.ProcessParent;
        this.childrenCurNodeIter = treeNode.children.iterator();
    }

    private ProcessStages doNext;
    private TreeNode next;
    private Iterator<TreeNode> childrenCurNodeIter;
    private Iterator<TreeNode> childrenSubNodeIter;

    @Override
    public boolean hasNext()
    {

        if (this.doNext == ProcessStages.ProcessParent)
        {
            this.next = this.treeNode;
            this.doNext = ProcessStages.ProcessChildCurNode;
            return true;
        }

        if (this.doNext == ProcessStages.ProcessChildCurNode)
        {
            if (childrenCurNodeIter.hasNext())
            {
                TreeNode childDirect = childrenCurNodeIter.next();
                childrenSubNodeIter = childDirect.iterator();
                this.doNext = ProcessStages.ProcessChildSubNode;
                return hasNext();
            }

            else
            {
                this.doNext = null;
                return false;
            }
        }

        if (this.doNext == ProcessStages.ProcessChildSubNode)
        {
            if (childrenSubNodeIter.hasNext())
            {
                this.next = childrenSubNodeIter.next();
                return true;
            }
            else
            {
                this.next = null;
                this.doNext = ProcessStages.ProcessChildCurNode;
                return hasNext();
            }
        }

        return false;
    }

    @Override
    public TreeNode next()
    {
        return this.next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}